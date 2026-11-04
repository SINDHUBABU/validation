import hashlib
import logging
import re
import time

import requests
from requests.auth import HTTPBasicAuth

from config import get_config
from adt.diagnostics import redact_sensitive_text


logger = logging.getLogger(__name__)


class QueryExecutor:
    """
    Executes read-only queries through the custom SAP ADT
    data-preview endpoint.
    """

    # ABAP source lines are limited to 255 characters. The data-preview
    # handler appears to pass the query text through an ABAP parser, so
    # any single line longer than this is rejected with a confusing
    # HTTP 400 grammar error.
    MAX_LINE_LENGTH = 255

    def __init__(self, enforce_line_limit=True):
        self.config = get_config()

        # When True, queries with an over-long line are rejected locally
        # with a clear message. Diagnostic scripts can set this to False
        # to deliberately send such queries to SAP.
        self.enforce_line_limit = enforce_line_limit

        self.base_url = self.config["base_url"].strip().rstrip("/")
        self.username = self.config["username"]
        self.password = self.config["password"]
        self.row_limit = self.config["row_limit"]

        # Reuse one session for authentication, cookies,
        # CSRF token retrieval, and query execution.
        self.session = requests.Session()

        self.session.auth = HTTPBasicAuth(
            self.username,
            self.password,
        )

        self.session.headers.update({
            "sap-client": "100",
            "Cache-Control": "no-cache",
        })

        # Custom ABAP data-preview endpoint.
        self.data_endpoint = (
            f"{self.base_url}"
            "/sap/bc/adt/datapreview/freestyle"
        )

    def _fetch_csrf_token(self, cds_entity=None):
        """
        Fetch a CSRF token from SAP.

        The same requests.Session must be used for the subsequent
        POST request so that SAP cookies are preserved.
        """

        if cds_entity is not None:
            if not isinstance(cds_entity, str) or not re.fullmatch(
                r"[A-Za-z][A-Za-z0-9_]*", cds_entity.strip()
            ):
                raise ValueError("Invalid CDS entity for ADT CSRF token discovery.")
            csrf_endpoint = (
                f"{self.base_url}/sap/bc/adt/ddic/ddl/sources/"
                f"{cds_entity.strip().lower()}/source/main"
            )
        else:
            raise ValueError("A verified CDS entity is required to fetch the ADT CSRF token.")

        try:
            started_at = time.monotonic()
            logger.info("adt_stage=csrf_request_started entity=%s", cds_entity.strip())
            response = self.session.get(
                csrf_endpoint,
                headers={
                    "Accept": "text/plain",
                    "X-CSRF-Token": "Fetch",
                },
                timeout=30,
            )

            if not response.ok:
                body = redact_sensitive_text(response.text)
                logger.warning(
                    "adt_stage=csrf_response_failed entity=%s http_status=%s duration_ms=%s response=%s",
                    cds_entity.strip(),
                    response.status_code,
                    round((time.monotonic() - started_at) * 1000),
                    body,
                )
                raise RuntimeError(
                    "Failed to fetch SAP CSRF token. "
                    f"HTTP {response.status_code}. "
                    f"Response: {body}"
                )

            token = response.headers.get("X-CSRF-Token")

            if not token or token.lower() == "required":
                raise RuntimeError(
                    "SAP did not return a usable CSRF token."
                )

            logger.info(
                "adt_stage=csrf_response_succeeded entity=%s http_status=%s duration_ms=%s",
                cds_entity.strip(),
                response.status_code,
                round((time.monotonic() - started_at) * 1000),
            )
            return token

        except requests.RequestException as exc:
            error = redact_sensitive_text(exc)
            logger.warning(
                "adt_stage=csrf_request_failed entity=%s error_type=%s error=%s",
                cds_entity.strip(),
                type(exc).__name__,
                error,
            )
            raise RuntimeError(
                f"Error connecting to SAP for CSRF token: {error}"
            ) from exc

    def _check_line_lengths(self, query):
        """Reject queries containing a line longer than the ABAP limit."""

        for line_no, line in enumerate(query.splitlines(), start=1):
            if len(line) > self.MAX_LINE_LENGTH:
                raise ValueError(
                    f"Query line {line_no} is {len(line)} characters; "
                    f"the ABAP source-line limit is "
                    f"{self.MAX_LINE_LENGTH}. Break the query across "
                    "multiple lines (for example, one field per line)."
                )

    def execute(self, query, row_limit=None, csrf_entity=None):
        """
        Execute a query through the SAP data-preview endpoint.

        Parameters
        ----------
        query : str
            ABAP Open SQL query supported by the custom handler.

        row_limit : int, optional
            Maximum number of rows requested.

        Returns
        -------
        str
            Raw XML response from SAP.
        """

        if not isinstance(query, str) or not query.strip():
            raise ValueError("Query cannot be empty.")

        query = query.strip()
        query_hash = hashlib.sha256(query.encode("utf-8")).hexdigest()

        limit = self.row_limit if row_limit is None else row_limit

        if (
            isinstance(limit, bool)
            or not isinstance(limit, int)
            or limit < 1
        ):
            logger.warning("adt_stage=query_rejected reason=invalid_row_limit")
            raise ValueError(
                "row_limit must be a positive integer."
            )

        if csrf_entity is None:
            source_match = re.search(
                r'(?i)\b(?:FROM|JOIN)\s+"?([A-Za-z][A-Za-z0-9_]*)"?',
                query,
            )
            csrf_entity = source_match.group(1) if source_match else None
        if csrf_entity is None:
            logger.warning(
                "adt_stage=query_rejected reason=source_entity_not_identified query_sha256=%s",
                query_hash,
            )
            raise ValueError("Could not identify a CDS entity for ADT CSRF token discovery.")
        logger.info(
            "adt_stage=query_received entity=%s query_sha256=%s query_chars=%s row_limit=%s",
            csrf_entity,
            query_hash,
            len(query),
            limit,
        )
        if self.enforce_line_limit:
            try:
                self._check_line_lengths(query)
            except ValueError:
                logger.warning(
                    "adt_stage=query_rejected reason=source_line_limit entity=%s query_sha256=%s",
                    csrf_entity,
                    query_hash,
                )
                raise
        csrf_token = self._fetch_csrf_token(cds_entity=csrf_entity)

        try:
            started_at = time.monotonic()
            logger.info(
                "adt_stage=data_preview_request_started entity=%s query_sha256=%s row_limit=%s",
                csrf_entity,
                query_hash,
                limit,
            )
            response = self.session.post(
                self.data_endpoint,
                headers={
                    "X-CSRF-Token": csrf_token,
                    "Content-Type": "text/plain; charset=utf-8",
                    "Accept": (
                        "application/vnd.sap.adt."
                        "datapreview.table.v1+xml"
                    ),
                },
                params={
                    "rowNumber": limit,
                },
                data=query,
                timeout=60,
            )

        except requests.RequestException as exc:
            error = redact_sensitive_text(exc)
            logger.warning(
                "adt_stage=data_preview_request_failed entity=%s query_sha256=%s error_type=%s error=%s",
                csrf_entity,
                query_hash,
                type(exc).__name__,
                error,
            )
            raise RuntimeError(
                f"Error connecting to SAP data-preview endpoint: {error}"
            ) from exc

        duration_ms = round((time.monotonic() - started_at) * 1000)
        if not response.ok:
            response_body = redact_sensitive_text(response.text).strip()
            logger.warning(
                "adt_stage=data_preview_response_failed entity=%s query_sha256=%s http_status=%s duration_ms=%s response=%s",
                csrf_entity,
                query_hash,
                response.status_code,
                duration_ms,
                response_body,
            )
            body_detail = (
                f" ADT response: {response_body}"
                if response_body else ""
            )
            raise RuntimeError(
                "SAP ADT data-preview request failed with "
                f"HTTP {response.status_code} for the verified CDS source."
                f"{body_detail}"
            )

        logger.info(
            "adt_stage=data_preview_response_succeeded entity=%s query_sha256=%s http_status=%s duration_ms=%s",
            csrf_entity,
            query_hash,
            response.status_code,
            duration_ms,
        )
        return response.text

    def close(self):
        """Close the HTTP session and release its resources."""
        self.session.close()

    def __enter__(self):
        """Allow QueryExecutor to be used as a context manager."""
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Close the session when leaving a context manager."""
        self.close()
