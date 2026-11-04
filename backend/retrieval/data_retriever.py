import hashlib
import logging
import re
import time
from itertools import combinations

from adt.query_executor import QueryExecutor
from adt.response_parser import parse_data_preview
from adt.cds_metadata import CDSMetadataClient
from config import get_config


logger = logging.getLogger(__name__)


class ExtractionError(RuntimeError):
    """Raised when a full extraction cannot continue safely."""


class ExtractionIntegrityError(ExtractionError):
    """
    Raised in strict mode when an extraction cannot be shown to be
    complete. The extraction summary is available as `.summary`.
    """

    def __init__(self, message, summary=None):
        super().__init__(message)
        self.summary = summary


class DataRetriever:
    """
    Retrieves SAP data dynamically and returns structured results.

    Features:
    - Read-only SELECT queries
    - SAP table and CDS view retrieval
    - Dynamic field selection
    - Parameterized CDS entity support
    - CDS metadata and parameter discovery
    - CDS parameter validation
    - Row-limit validation
    - XML response parsing

    Note:
    Application-side query validation is a basic safeguard.
    The SAP execution handler must independently enforce
    read-only access and authorization.

    Query formatting:
    The SAP data-preview handler rejects any query line longer than
    255 characters (ABAP source-line limit). All queries built here
    therefore put each projected field on its own line.
    """

    IDENTIFIER_PATTERN = re.compile(
        r"^[A-Za-z][A-Za-z0-9_]*$"
    )

    FORBIDDEN_SQL_PATTERN = re.compile(
        r"\b(INSERT|UPDATE|DELETE|MODIFY|UPSERT|MERGE|"
        r"DROP|ALTER|CREATE|TRUNCATE|EXECUTE|CALL)\b",
        re.IGNORECASE,
    )

    def __init__(
        self,
        default_row_limit=100,
        max_row_limit=1000,
    ):
        if (
            isinstance(default_row_limit, bool)
            or not isinstance(default_row_limit, int)
            or default_row_limit < 1
        ):
            raise ValueError(
                "default_row_limit must be a positive integer."
            )

        if (
            isinstance(max_row_limit, bool)
            or not isinstance(max_row_limit, int)
            or max_row_limit < 1
        ):
            raise ValueError(
                "max_row_limit must be a positive integer."
            )

        if default_row_limit > max_row_limit:
            raise ValueError(
                "default_row_limit cannot exceed max_row_limit."
            )

        self.default_row_limit = default_row_limit
        self.max_row_limit = max_row_limit

        self.executor = QueryExecutor()

    # ---------------------------------------------------------
    # IDENTIFIER VALIDATION
    # ---------------------------------------------------------

    @classmethod
    def _validate_identifier(
        cls,
        value,
        label="identifier",
    ):
        """Validate a SAP object, field, or parameter name."""

        if not isinstance(value, str):
            raise ValueError(
                f"{label} must be a string."
            )

        value = value.strip()

        if not cls.IDENTIFIER_PATTERN.fullmatch(value):
            raise ValueError(
                f"Invalid {label}: {value!r}"
            )

        return value

    # ---------------------------------------------------------
    # ROW LIMIT VALIDATION
    # ---------------------------------------------------------

    def _validate_row_limit(self, row_limit):
        """Validate the requested maximum number of rows."""

        if row_limit is None:
            row_limit = self.default_row_limit

        if (
            isinstance(row_limit, bool)
            or not isinstance(row_limit, int)
            or row_limit < 1
            or row_limit > self.max_row_limit
        ):
            raise ValueError(
                f"row_limit must be between 1 and "
                f"{self.max_row_limit}."
            )

        return row_limit

    # ---------------------------------------------------------
    # CDS PARAMETER VALUE FORMATTING
    # ---------------------------------------------------------

    @staticmethod
    def _format_parameter_value(value):
        """
        Format a Python value as an ABAP Open SQL literal.

        Strings are quoted and embedded apostrophes are escaped.
        Integers are emitted without quotes.

        Floating-point values are intentionally rejected.
        """

        if isinstance(value, bool) or value is None:
            raise ValueError(
                "CDS parameters cannot be None or boolean values."
            )

        if isinstance(value, str):
            escaped = value.replace("'", "''")
            return f"'{escaped}'"

        if isinstance(value, int):
            return str(value)

        if isinstance(value, float):
            raise ValueError(
                "Floating-point CDS parameters are not supported "
                "by this formatter. Supply a correctly formatted "
                "string or integer instead."
            )

        raise ValueError(
            "Unsupported CDS parameter type: "
            f"{type(value).__name__}"
        )

    # ---------------------------------------------------------
    # BASIC QUERY VALIDATION
    # ---------------------------------------------------------

    def _validate_query(self, query):
        """
        Perform basic validation before query execution.

        This is not a complete SQL parser or a security boundary.
        """

        if not isinstance(query, str) or not query.strip():
            raise ValueError("Query cannot be empty.")

        query = query.strip()

        if not re.match(
            r"^SELECT\b",
            query,
            re.IGNORECASE,
        ):
            raise ValueError(
                "Only SELECT queries are permitted."
            )

        if self.FORBIDDEN_SQL_PATTERN.search(query):
            raise ValueError(
                "Query contains a forbidden SQL keyword."
            )

        if ";" in query:
            raise ValueError(
                "Multiple SQL statements are not permitted."
            )

        return query

    # ---------------------------------------------------------
    # CDS METADATA DISCOVERY
    # ---------------------------------------------------------

    def discover_view_parameters(self, view_name):
        """
        Discover declared parameters for a CDS entity.

        Returns:
            {
                "entity": "...",
                "parameters": [
                    {"name": "P_COUNTRY", "type": "land1"}
                ],
                "parameterized": True
            }
        """

        view_name = self._validate_identifier(
            view_name,
            label="CDS entity name",
        )

        config = get_config()

        with CDSMetadataClient(
            base_url=config["base_url"],
            username=config["username"],
            password=config["password"],
        ) as metadata_client:

            return metadata_client.discover_parameters(
                view_name
            )

    # ---------------------------------------------------------
    # CDS PARAMETER VALIDATION
    # ---------------------------------------------------------

    def validate_view_parameters(
        self,
        view_name,
        parameters=None,
    ):
        """
        Validate supplied parameter names against CDS metadata.

        Checks:
        - Missing required parameters
        - Unexpected parameters
        - Duplicate names after case normalization
        - Values supported by the current literal formatter

        This method does not execute a data query.
        """

        view_name = self._validate_identifier(
            view_name,
            label="CDS entity name",
        )

        metadata = self.discover_view_parameters(
            view_name
        )

        expected = {
            item["name"].upper(): item["type"]
            for item in metadata["parameters"]
        }

        if parameters is None:
            parameters = {}

        if not isinstance(parameters, dict):
            raise ValueError(
                "parameters must be a dictionary."
            )

        supplied = {}

        for name, value in parameters.items():

            validated_name = self._validate_identifier(
                name,
                label="parameter name",
            ).upper()

            if validated_name in supplied:
                raise ValueError(
                    "Duplicate parameter name after "
                    f"case normalization: {name}"
                )

            supplied[validated_name] = value

        missing = sorted(
            set(expected) - set(supplied)
        )

        unexpected = sorted(
            set(supplied) - set(expected)
        )

        errors = []

        if missing:
            errors.append(
                "Missing required parameters: "
                + ", ".join(missing)
            )

        if unexpected:
            errors.append(
                "Unexpected parameters: "
                + ", ".join(unexpected)
            )

        if errors:
            raise ValueError("; ".join(errors))

        # Validate that values can be represented by the
        # current parameter-literal formatter.
        for value in supplied.values():
            self._format_parameter_value(value)

        return {
            "entity": metadata["entity"],
            "parameterized": metadata["parameterized"],
            "validated_parameters": supplied,
            "parameter_types": expected,
            "valid": True,
        }

    # ---------------------------------------------------------
    # GENERIC QUERY EXECUTION
    # ---------------------------------------------------------

    def fetch_query(
        self,
        query,
        row_limit=None,
        csrf_entity=None,
    ):
        """
        Execute a supported SELECT query and parse its XML result.
        """

        query = self._validate_query(query)

        limit = self._validate_row_limit(
            row_limit
        )

        if csrf_entity:
            xml_response = self.executor.execute(
                query=query,
                row_limit=limit,
                csrf_entity=csrf_entity,
            )
        else:
            xml_response = self.executor.execute(
                query=query,
                row_limit=limit,
            )

        query_hash = hashlib.sha256(query.encode("utf-8")).hexdigest()
        try:
            result = parse_data_preview(xml_response)
        except ValueError as exc:
            logger.warning(
                "adt_stage=data_preview_parse_failed query_sha256=%s error_type=%s",
                query_hash,
                type(exc).__name__,
            )
            raise

        returned_rows = len(result["rows"])
        total_rows = result["total_rows"]
        logger.info(
            "adt_stage=data_preview_parsed query_sha256=%s total_rows=%s returned_rows=%s truncated=%s",
            query_hash,
            total_rows,
            returned_rows,
            total_rows > returned_rows,
        )
        return result

    # ---------------------------------------------------------
    # TABLE / CDS VIEW RETRIEVAL
    # ---------------------------------------------------------

    def fetch_view(
        self,
        view_name,
        row_limit=None,
        fields=None,
        parameters=None,
    ):
        """
        Retrieve data from a supported SAP table or CDS view.

        Parameters:
            view_name:
                SAP table or CDS entity name.

            row_limit:
                Maximum number of rows to retrieve.

            fields:
                Optional list of fields to retrieve.

            parameters:
                Optional CDS parameter dictionary.

        Parameterized CDS entities are validated against their
        source metadata before the data query is executed.

        Each projected field is placed on its own line so that no
        query line exceeds the 255-character ABAP limit.
        """

        view_name = self._validate_identifier(
            view_name,
            label="SAP object name",
        )

        limit = self._validate_row_limit(
            row_limit
        )

        # -----------------------------------------------------
        # Validate selected fields
        # -----------------------------------------------------

        if fields is None:
            select_fields = "*"

        else:
            if (
                not isinstance(fields, (list, tuple))
                or not fields
            ):
                raise ValueError(
                    "fields must be a non-empty list or tuple."
                )

            validated_fields = [
                self._validate_identifier(
                    field,
                    label="field name",
                )
                for field in fields
            ]

            # Parameterized CDS projections use qualified
            # field names, matching the tested Eclipse ADT query.
            if parameters is not None:
                projected = [
                    f"{view_name}~{field}"
                    for field in validated_fields
                ]

            else:
                projected = validated_fields

            select_fields = ",\n    ".join(projected)

        # -----------------------------------------------------
        # Validate and construct CDS parameters
        # -----------------------------------------------------

        if parameters is None:
            source = view_name

        else:
            validation = self.validate_view_parameters(
                view_name,
                parameters,
            )

            validated_parameters = (
                validation["validated_parameters"]
            )

            # Preserve the metadata declaration's parameter
            # names rather than using caller-provided casing.
            declared_names = {
                name.upper(): name
                for name in validation["parameter_types"]
            }

            assignments = []

            for normalized_name, value in (
                validated_parameters.items()
            ):
                parameter_name = declared_names[
                    normalized_name
                ]

                formatted_value = (
                    self._format_parameter_value(value)
                )

                assignments.append(
                    f"{parameter_name} = {formatted_value}"
                )

            source = (
                f"{view_name}(\n    "
                + ",\n    ".join(assignments)
                + "\n)"
            )

        # -----------------------------------------------------
        # Construct and execute query
        # -----------------------------------------------------

        query = (
            f"SELECT\n    {select_fields}\nFROM {source}"
        )

        return self.fetch_query(
            query=query,
            row_limit=limit,
            csrf_entity=view_name,
        )

    # ---------------------------------------------------------
    # AUTO RETRIEVAL - discovers everything itself
    # ---------------------------------------------------------

    def fetch_view_auto(
        self,
        view_name,
        row_limit=None,
        field_selection=None,
        parameter_overrides=None,
    ):
        """Discover CDS metadata, retrieve data, and return both."""

        from datetime import datetime, timezone

        # NOTE: confirm this import path against the real project
        # tree. The handoff listed retrieval/auto_query.py, but this
        try:
            from reconciliation.auto_query import build_select
        except ImportError:
            from auto_query import build_select

        view_name = self._validate_identifier(
            view_name,
            label="CDS entity name",
        )

        limit = self._validate_row_limit(row_limit)
        config = get_config()

        # 1. Discover metadata
        with CDSMetadataClient(
            base_url=config["base_url"],
            username=config["username"],
            password=config["password"],
        ) as metadata_client:
            metadata = metadata_client.discover_metadata(view_name)

        # 2. Build the query
        query, resolved_params = build_select(
            view_name=view_name,
            metadata=metadata,
            overrides=parameter_overrides,
            field_selection=field_selection,
        )

        # 3. Execute the query
        query = self._validate_query(query)

        xml_response = self.executor.execute(
            query=query,
            row_limit=limit,
            csrf_entity=view_name,
        )

        # 4. Parse SAP's response
        result = parse_data_preview(xml_response)

        discovered_fields = metadata.get("fields", [])
        rows = result.get("rows", [])

        # 5. Return data and metadata together
        result.update({
            "entity": view_name,

            "metadata": {
                "fields": discovered_fields,
                "key_fields": metadata.get("key_fields", []),
                "parameters": metadata.get("parameters", []),
                "parameterized": metadata.get(
                    "parameterized", False
                ),
            },

            "data": {
                "rows": rows,
                "row_count": len(rows),
                "total_rows": result.get("total_rows"),
            },

            "retrieval": {
                "executed_query": query,
                "row_limit": limit,
                "resolved_parameters": resolved_params,
                "retrieved_at": datetime.now(
                    timezone.utc
                ).isoformat(),
            },

            # Preserve compatibility with existing tests.
            "resolved_parameters": resolved_params,
            "discovered_fields": [
                field["name"] for field in discovered_fields
            ],
            "executed_query": query,
        })

        return result

    # ---------------------------------------------------------
    # FULL EXTRACTION (keyset pagination)
    # ---------------------------------------------------------
    #
    # OFFSET is rejected by the data-preview endpoint, so batches are
    # fetched by ordering on the key and filtering for rows strictly
    # after the last key already received:
    #
    #   WHERE k1 > v1
    #      OR (k1 = v1 AND k2 > v2)
    #      OR (k1 = v1 AND k2 = v2 AND k3 > v3) ...
    #   ORDER BY k1, k2, k3 ...
    #
    # Completeness is checked with SAP's own counts: for every batch,
    # the reported number of matching rows must equal the initial total
    # minus the rows already extracted. A gap, an overlap, a duplicate
    # key at a batch boundary, or a change in the source during the
    # extraction all break that equality.

    @staticmethod
    def _sql_string_literal(value):
        """Quote a value as an ABAP Open SQL character literal."""

        return "'" + str(value).replace("'", "''") + "'"

    @staticmethod
    def _qualified_name(view_name, field):
        """Return ENTITY~FIELD in the uppercase form tested on SAP."""

        return f"{view_name.upper()}~{field.upper()}"

    @classmethod
    def _build_keyset_where(cls, view_name, key_fields, cursor):
        """
        Build the condition selecting rows strictly after `cursor`
        in key order.
        """

        if len(cursor) != len(key_fields):
            raise ValueError(
                "cursor must have one value per key field."
            )

        names = [
            cls._qualified_name(view_name, key)
            for key in key_fields
        ]

        literals = [
            cls._sql_string_literal(value)
            for value in cursor
        ]

        lines = []

        for i in range(len(names)):
            conditions = [
                f"{names[j]} = {literals[j]}"
                for j in range(i)
            ]
            conditions.append(f"{names[i]} > {literals[i]}")

            if i == 0:
                lines.append(conditions[0])
            else:
                lines.append("OR (")
                lines.append(
                    "    " + "\n    AND ".join(conditions)
                )
                lines.append(")")

        return "\n".join(lines)

    @classmethod
    def _build_extraction_query(
        cls,
        view_name,
        fields,
        key_fields,
        cursor,
    ):
        """Build one keyset-paginated batch query (one field per line)."""

        projected = ",\n    ".join(
            cls._qualified_name(view_name, field)
            for field in fields
        )

        query = f"SELECT\n    {projected}\nFROM {view_name}"

        if cursor is not None:
            where = cls._build_keyset_where(
                view_name,
                key_fields,
                cursor,
            )
            indented = "\n".join(
                "    " + line for line in where.splitlines()
            )
            query += "\nWHERE\n" + indented

        order_by = ",\n    ".join(
            cls._qualified_name(view_name, key)
            for key in key_fields
        )

        query += f"\nORDER BY\n    {order_by}"

        return query

    def discover_columns(self, view_name):
        """
        Return SAP's own column list for a table or non-parameterized
        CDS view, as reported by the data-preview response.

        This is the authoritative field list: it covers every field of
        the view, whereas the regex-based DDL parser can miss fields.

        Each item: {"name", "type", "description", "is_key",
        "is_key_figure"}. Names are uppercase.
        """

        view_name = self._validate_identifier(
            view_name,
            label="SAP object name",
        )

        result = self.fetch_query(
            query=f"SELECT * FROM {view_name}",
            row_limit=1,
        )

        return result.get("columns", [])

    def _discover_metadata(self, view_name):
        """Discover CDS metadata (separate method so tests can stub it)."""

        config = get_config()

        with CDSMetadataClient(
            base_url=config["base_url"],
            username=config["username"],
            password=config["password"],
        ) as metadata_client:
            return metadata_client.discover_metadata(view_name)

    def _fetch_batch(
        self,
        view_name,
        fields,
        key_fields,
        cursor,
        batch_size,
    ):
        """Fetch one batch (separate method so tests can stub it)."""

        query = self._build_extraction_query(
            view_name,
            fields,
            key_fields,
            cursor,
        )

        return self.fetch_query(
            query=query,
            row_limit=batch_size,
        )

    def _infer_pagination_key(self, sap_columns, sample_rows, max_key_fields=3):
        """Infer a candidate non-null unique cursor from a sample.

        This is a heuristic, not proof of global uniqueness. fetch_all
        subsequently validates the candidate against every extracted row
        and SAP's remaining-row counts, failing closed on inconsistencies.
        """
        if not sample_rows:
            return None

        row_count = len(sample_rows)
        candidates = []

        for column in sap_columns:
            name = column["name"].upper()
            values = [row.get(name) for row in sample_rows]
            if any(value is None or str(value).strip() == "" for value in values):
                continue
            distinct_count = len(set(str(value) for value in values))
            if distinct_count > 1:
                candidates.append((name, values, distinct_count))

        # Test high-cardinality columns first; this avoids needlessly
        # testing every possible combination when a short key is available.
        candidates.sort(key=lambda item: item[2], reverse=True)

        for name, values, _ in candidates:
            if len(set(str(value) for value in values)) == row_count:
                return [name]

        # Composite keys are tested only with fields that are non-null in
        # every sampled row. The full extraction still validates the choice.
        max_size = min(max_key_fields, len(candidates))
        if len(candidates) > 60:
            max_size = min(max_size, 2)

        for size in range(2, max_size + 1):
            for group in combinations(candidates, size):
                tuples = zip(*(item[1] for item in group))
                if len(set(tuple(str(value) for value in values) for values in tuples)) == row_count:
                    return [item[0] for item in group]

        return None

    def fetch_all(
        self,
        view_name,
        fields=None,
        key_fields=None,
        batch_size=5000,
        on_batch=None,
        strict=True,
        verify_unique=True,
    ):
        """
        Extract every row of a non-parameterized CDS view or table.

        Parameters:
            view_name:
                CDS entity name.

            fields:
                Fields to retrieve (default: every column SAP reports
                for the view, which can be more than the DDL parser
                finds).
                Key fields are always included; any that are missing
                are added at the front.

            key_fields:
                Fields that uniquely identify a row, in sort order
                (default: key fields discovered from the CDS source).

            batch_size:
                Rows per request. Must not exceed max_row_limit, so
                construct the retriever with a suitable max_row_limit.

            on_batch:
                Optional callable(rows, info). When given, batches are
                handed to it and NOT accumulated, so memory stays flat.

            strict:
                If True (default), raise ExtractionIntegrityError when
                the extraction cannot be shown to be complete.

            verify_unique:
                If True (default), remember every key seen and report
                duplicates. Uses memory proportional to the row count.

        Returns a dictionary with the rows (empty when streaming),
        row counts, batch count, a `complete` flag and a list of
        `issues`.
        """

        view_name = self._validate_identifier(
            view_name,
            label="CDS entity name",
        )

        batch_size = self._validate_row_limit(batch_size)

        metadata = self._discover_metadata(view_name)

        if metadata.get("parameterized"):
            raise NotImplementedError(
                "fetch_all does not support parameterized CDS views yet."
            )

        # -----------------------------------------------------
        # Resolve columns, projection, and pagination key
        # -----------------------------------------------------
        sap_columns = self.discover_columns(view_name)
        if not sap_columns:
            raise ExtractionError(
                f"SAP returned no column metadata for {view_name}."
            )

        if key_fields is None:
            key_fields = list(metadata.get("key_fields", []))
        key_fields = [
            self._validate_identifier(key, label="key field")
            for key in key_fields
        ]

        if fields is None:
            fields = [column["name"] for column in sap_columns]
        fields = [
            self._validate_identifier(field, label="field name")
            for field in fields
        ]

        selected = []
        seen_names = set()
        for field in fields:
            if field.upper() not in seen_names:
                seen_names.add(field.upper())
                selected.append(field)

        sap_names = {column["name"].upper() for column in sap_columns}
        unknown = [field for field in selected if field.upper() not in sap_names]
        if unknown:
            raise ValueError(
                f"Field(s) not found in {view_name} according to SAP: "
                + ", ".join(unknown)
            )

        # Validate metadata-declared keys against an actual SAP sample.
        # CDS metadata may mark fields as keys even when a projection/join
        # produces repeated values. If the declared key is not unique, try
        # another non-null candidate from the sample. The extraction loop
        # still checks every batch and fails closed if the candidate is not
        # globally unique.
        if key_fields:
            probe_fields = [column["name"] for column in sap_columns]
            probe_projection = ",\n    ".join(
                self._qualified_name(view_name, field)
                for field in probe_fields
            )
            probe_query = (
                f"SELECT\n    {probe_projection}\nFROM {view_name}"
            )
            key_probe = self.fetch_query(
                query=probe_query,
                row_limit=batch_size,
            )
            key_probe_rows = key_probe.get("rows", [])
            declared_names = [key.upper() for key in key_fields]

            def key_is_unique(rows, names):
                values = []
                for row in rows:
                    current = tuple(row.get(name) for name in names)
                    if any(value is None for value in current):
                        return False
                    values.append(current)
                return len(values) == len(set(values))

            if key_probe_rows and not key_is_unique(
                key_probe_rows, declared_names
            ):
                inferred = self._infer_pagination_key(
                    sap_columns,
                    key_probe_rows,
                    max_key_fields=3,
                )
                if inferred and key_is_unique(key_probe_rows, inferred):
                    key_fields = inferred
                else:
                    raise ExtractionError(
                        f"Cannot safely paginate {view_name}: the "
                        f"metadata-declared key ({', '.join(key_fields)}) "
                        f"is not unique in SAP's first {len(key_probe_rows)} "
                        "rows, and no alternative non-null unique key could "
                        "be inferred from that sample. Expose a genuinely "
                        "unique, stable row key in the CDS view and pass it "
                        "through key_fields."
                    )

        started = time.perf_counter()

        # If no key is declared, request one sample containing every SAP
        # column. This also lets us return small datasets in one request
        # without inventing a pagination key.
        if not key_fields:
            probe_fields = [column["name"] for column in sap_columns]
            probe_projection = ",\n    ".join(
                self._qualified_name(view_name, field)
                for field in probe_fields
            )
            probe_query = f"SELECT\n    {probe_projection}\nFROM {view_name}"
            probe = self.fetch_query(query=probe_query, row_limit=batch_size)
            probe_rows = probe.get("rows", [])
            probe_total = probe.get("total_rows")

            if (
                isinstance(probe_total, int)
                and not isinstance(probe_total, bool)
                and probe_total >= 0
                and probe_total == len(probe_rows)
            ):
                projected_rows = [
                    {name.upper(): row.get(name.upper()) for name in selected}
                    for row in probe_rows
                ]
                if on_batch is not None and projected_rows:
                    on_batch(
                        projected_rows,
                        {
                            "batch": 1,
                            "rows": len(projected_rows),
                            "extracted_so_far": len(projected_rows),
                            "total_rows": probe_total,
                        },
                    )
                return {
                    "entity": view_name,
                    "rows": projected_rows if on_batch is None else [],
                    "streamed": on_batch is not None,
                    "row_count": len(projected_rows),
                    "total_rows_reported": probe_total,
                    "batches": 1,
                    "batch_size": batch_size,
                    "key_fields": [],
                    "fields": selected,
                    "added_key_fields": [],
                    "column_types": {
                        column["name"].upper(): column.get("type")
                        for column in sap_columns
                        if column["name"].upper() in {name.upper() for name in selected}
                    },
                    "complete": True,
                    "issues": [],
                    "elapsed_seconds": time.perf_counter() - started,
                }

            # Do NOT infer a cursor from an unordered preview sample.
            # SAP can return a different subset/order for the subsequent
            # ORDER BY query. A key that looks unique in the probe may be
            # duplicated in the actual ordered batch (as happened with
            # Z20_OrderToCash), which can silently skip rows during keyset
            # pagination. For datasets larger than one request, require a
            # declared/stable key or one supplied explicitly by the caller.
            raise ExtractionError(
                f"Cannot safely paginate {view_name}: SAP metadata did not "
                "declare a key, and the dataset is larger than one request. "
                "The preview sample is unordered, so inferring a cursor from "
                "it cannot guarantee complete extraction. Pass key_fields="
                "with a genuinely unique, stable key, or expose one in the "
                "CDS view."
            )

        missing_keys = [key for key in key_fields if key.upper() not in seen_names]
        selected = missing_keys + selected
        selected_upper = {field.upper() for field in selected}
        unknown = [field for field in selected if field.upper() not in sap_names]
        if unknown:
            raise ValueError(
                f"Field(s) not found in {view_name} according to SAP: "
                + ", ".join(unknown)
            )

        column_types = {
            column["name"].upper(): column.get("type")
            for column in sap_columns
            if column["name"].upper() in selected_upper
        }
        key_names = [key.upper() for key in key_fields]

        # -----------------------------------------------------
        # Extraction loop
        # -----------------------------------------------------

        collected = [] if on_batch is None else None
        seen_keys = set() if verify_unique else None
        issues = []

        cursor = None
        extracted = 0
        batches = 0
        initial_total = None
        while True:
            result = self._fetch_batch(
                view_name,
                selected,
                key_fields,
                cursor,
                batch_size,
            )

            rows = result.get("rows", [])
            reported = result.get("total_rows")
            batches += 1

            # Independent completeness check against SAP's count.
            if initial_total is None:
                initial_total = reported
                if reported is not None and (
                    isinstance(reported, bool)
                    or not isinstance(reported, int)
                    or reported < 0
                ):
                    issues.append(
                        f"SAP returned an invalid initial total_rows value: {reported!r}."
                    )
            else:
                expected_remaining = (
                    initial_total - extracted
                    if isinstance(initial_total, int)
                    and not isinstance(initial_total, bool)
                    else None
                )

                if expected_remaining is not None and reported != expected_remaining:
                    issues.append(
                        f"Batch {batches}: SAP reports {reported} "
                        f"matching rows after the cursor, expected "
                        f"{expected_remaining}."
                    )

            if not rows:
                break

            batch_keys = []

            for row in rows:
                key_values = []

                for name in key_names:
                    if name not in row:
                        raise ExtractionError(
                            f"Key field {name} missing from SAP "
                            "response."
                        )

                    value = row[name]

                    if value is None:
                        raise ExtractionError(
                            f"Key field {name} is empty/null in batch "
                            f"{batches}. The parser cannot tell NULL "
                            "from a blank value, so a safe cursor "
                            "cannot be built."
                        )

                    key_values.append(value)

                batch_keys.append(tuple(key_values))

            if seen_keys is not None:
                duplicates = 0

                for key in batch_keys:
                    if key in seen_keys:
                        duplicates += 1
                    else:
                        seen_keys.add(key)

                if duplicates:
                    issues.append(
                        f"Batch {batches}: {duplicates} duplicate "
                        "key(s) already seen in earlier batches."
                    )

            extracted += len(rows)

            if on_batch is not None:
                on_batch(
                    rows,
                    {
                        "batch": batches,
                        "rows": len(rows),
                        "extracted_so_far": extracted,
                        "total_rows": initial_total,
                    },
                )
            else:
                collected.extend(rows)

            # In strict mode the result will be rejected anyway, so
            # stop downloading as soon as an issue is known.
            if strict and issues:
                break

            # Do not stop merely because SAP returned fewer rows than
            # requested. Some ADT handlers cap page sizes independently
            # of rowNumber; stopping here would silently truncate data.
            # Continue from the last key until SAP returns an empty page.
            new_cursor = batch_keys[-1]

            if new_cursor == cursor:
                raise ExtractionError(
                    f"Cursor did not advance in batch {batches}; "
                    "stopping to avoid an infinite loop."
                )

            cursor = new_cursor

        if initial_total is None:
            issues.append(
                "SAP did not provide an initial total_rows count; "
                "the extraction cannot be independently verified as complete."
            )
        elif extracted != initial_total:
            issues.append(
                f"Extracted {extracted} rows but SAP initially "
                f"reported {initial_total}."
            )

        complete = not issues

        summary = {
            "entity": view_name,
            "rows": collected if collected is not None else [],
            "streamed": on_batch is not None,
            "row_count": extracted,
            "total_rows_reported": initial_total,
            "batches": batches,
            "batch_size": batch_size,
            "key_fields": key_fields,
            "fields": selected,
            "added_key_fields": missing_keys,
            "column_types": column_types,
            "complete": complete,
            "issues": issues,
            "elapsed_seconds": time.perf_counter() - started,
        }

        if strict and not complete:
            raise ExtractionIntegrityError(
                "Extraction could not be shown to be complete: "
                + " | ".join(issues),
                summary,
            )

        return summary

    # ---------------------------------------------------------
    # RESOURCE MANAGEMENT
    # ---------------------------------------------------------

    def close(self):
        """Close the underlying SAP HTTP session."""

        self.executor.close()

    def __enter__(self):
        """Support use as a context manager."""

        return self

    def __exit__(
        self,
        exc_type,
        exc_value,
        traceback,
    ):
        """Close resources when leaving a context manager."""

        self.close()
