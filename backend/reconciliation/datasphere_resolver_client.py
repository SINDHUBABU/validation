"""
Datasphere Dynamic Resolver Client
Fulfills the DatasphereDependencyResolver loader/reader protocol:
- list_views, list_local_tables, list_analytic_models, list_fact_models, list_remote_tables
- read_view, read_local_table, read_analytic_model, read_fact_model

100% Dynamic - ZERO hardcoded analytical models in source code.
All models, views, and tables are loaded dynamically from:
1. Datasphere SQLite space cache (DatasphereSpaceCache)
2. Live Datasphere CSN / Data Builder definitions
3. Runtime model registrations
"""
from __future__ import annotations

import json
import logging
import os
import re
import shutil
import subprocess
import tempfile
from typing import Any, Dict, List, Optional
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

from reconciliation.datasphere_space_cache import REPLICATION_SPACE_ID

logger = logging.getLogger("DatasphereResolverClient")


CLI_OBJECT_TYPES = {
    "view": "views",
    "local_table": "local-tables",
    "remote_table": "remote-tables",
    "analytic_model": "analytic-models",
    "fact_model": "fact-models",
}
CLI_IDENTIFIER = re.compile(r"^[A-Za-z][A-Za-z0-9_.-]*$")


class DatasphereResolverClient:
    """
    Datasphere Client implementation that fulfills the DatasphereDependencyResolver loader/reader protocol.
    Works dynamically across cached, discovered, and custom Analytical Models and Views.
    No hardcoded analytical models: all definitions are resolved dynamically from space cache,
    CSN artifacts, or runtime model registrations.
    """

    def __init__(self, space: str | None = None, cache=None):
        # Space IDs are configuration, not model-specific constants. Set
        # DATASPHERE_MODELING_SPACE in backend/.env or pass space explicitly.
        self.space = (space or os.getenv("DATASPHERE_MODELING_SPACE", "")).strip()
        if not self.space:
            raise RuntimeError(
                "Datasphere modeling space is not configured. Set "
                "DATASPHERE_MODELING_SPACE in backend/.env or pass space explicitly."
            )
        try:
            from config import resolve_datasphere_cli_path
            self.cli_path = resolve_datasphere_cli_path(os.getenv("DATASPHERE_CLI_COMMAND", "datasphere"))
        except Exception:
            self.cli_command = os.getenv("DATASPHERE_CLI_COMMAND", "datasphere").strip()
            self.cli_path = shutil.which(self.cli_command) if self.cli_command else None
        self.cli_available = bool(self.cli_path)
        self.accept_content_type = os.getenv(
            "DATASPHERE_CLI_ACCEPT",
            "application/vnd.sap.datasphere.object.content+json",
        ).strip()
        self.cli_reads: set[tuple[str, str]] = set()
        self._definition_cache: dict[tuple[str, str, str], dict] = {}
        if cache is None:
            try:
                from reconciliation.datasphere_space_cache import DatasphereSpaceCache
                self.cache = DatasphereSpaceCache()
            except Exception:
                try:
                    from datasphere_space_cache import DatasphereSpaceCache
                    self.cache = DatasphereSpaceCache()
                except Exception:
                    self.cache = None
        else:
            self.cache = cache

        self.models: Dict[str, dict] = {}
        self.views: Dict[str, dict] = {}
        self.local_tables: Dict[str, dict] = {}
        self.fact_models: Dict[str, dict] = {}
        self.remote_tables: List[dict] = []

        self._sync_from_cache(self.space)

    @staticmethod
    def _cli_auth_options_file() -> tuple[str, str] | None:
        """Create an ephemeral CLI options file from the backend's OAuth config."""
        from config import (
            DATASPHERE_API_URL,
            DATASPHERE_AUTHORIZATION_URL,
            DATASPHERE_TOKEN_URL,
            OAUTH_CLIENT_ID,
            OAUTH_CLIENT_SECRET,
        )

        client_id = os.getenv("OAUTH_CLIENT_ID", OAUTH_CLIENT_ID).strip()
        client_secret = os.getenv("OAUTH_CLIENT_SECRET", OAUTH_CLIENT_SECRET).strip()
        token_url = os.getenv("DATASPHERE_TOKEN_URL", DATASPHERE_TOKEN_URL).strip()
        api_url = os.getenv("DATASPHERE_API_URL", DATASPHERE_API_URL).strip()
        authorization_url = os.getenv(
            "DATASPHERE_AUTHORIZATION_URL", DATASPHERE_AUTHORIZATION_URL
        ).strip()
        if not any((client_id, client_secret, token_url, api_url)):
            return None
        if not all((client_id, client_secret, token_url, api_url)):
            raise RuntimeError(
                "Datasphere CLI client credentials are incomplete. Configure "
                "DATASPHERE_API_URL, DATASPHERE_TOKEN_URL, OAUTH_CLIENT_ID, "
                "and OAUTH_CLIENT_SECRET in backend/.env."
            )

        tenant = urlsplit(api_url)
        token = urlsplit(token_url)
        if tenant.scheme not in {"http", "https"} or not tenant.netloc:
            raise RuntimeError("DATASPHERE_API_URL must be an absolute HTTP(S) URL.")
        if token.scheme not in {"http", "https"} or not token.netloc:
            raise RuntimeError("DATASPHERE_TOKEN_URL must be an absolute HTTP(S) URL.")
        if not authorization_url:
            suffix = "/oauth/token"
            if not token.path.rstrip("/").endswith(suffix):
                raise RuntimeError(
                    "Set DATASPHERE_AUTHORIZATION_URL because the authorization "
                    "endpoint could not be inferred from DATASPHERE_TOKEN_URL."
                )
            authorization_path = token.path.rstrip("/")[:-len("token")] + "authorize"
            authorization_url = urlunsplit(
                (token.scheme, token.netloc, authorization_path, "", "")
            )

        options = {
            "client-id": client_id,
            "client-secret": client_secret,
            "token-url": token_url,
            "authorization-url": authorization_url,
            "authorization-flow": "client_credentials",
        }
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            suffix=".json",
            prefix="datasphere-cli-options-",
            delete=False,
        ) as options_file:
            json.dump(options, options_file)
            return urlunsplit((tenant.scheme, tenant.netloc, "", "", "")), options_file.name

    @staticmethod
    def _decode_cli_json(output: str) -> Any:
        decoder = json.JSONDecoder()
        for index, char in enumerate(output):
            if char not in "[{":
                continue
            try:
                value, _ = decoder.raw_decode(output[index:])
                return value
            except json.JSONDecodeError:
                continue
        raise ValueError("Datasphere CLI read command did not return JSON.")

    @staticmethod
    def _unwrap_cli_definition(value: Any) -> dict:
        if isinstance(value, dict):
            for key in ("definition", "object", "data", "value", "result"):
                nested = value.get(key)
                if isinstance(nested, dict):
                    return nested
            return value
        raise ValueError("Datasphere CLI returned an unexpected object definition.")

    @staticmethod
    def _unwrap_cli_list(value: Any) -> list[dict]:
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]
        if isinstance(value, dict):
            for key in ("value", "objects", "items", "data", "results", "entries"):
                nested = value.get(key)
                if isinstance(nested, list):
                    return [item for item in nested if isinstance(item, dict)]
        raise ValueError("Datasphere CLI returned an unexpected object list.")

    def _run_cli(self, args: list[str]) -> Any:
        if not self.cli_available:
            raise RuntimeError(
                "SAP Datasphere CLI is not available. Install/configure the "
                "Datasphere CLI and set DATASPHERE_CLI_COMMAND if needed."
            )
        auth_file: str | None = None
        try:
            auth_options = self._cli_auth_options_file()
            command = [self.cli_path or self.cli_command]
            if auth_options:
                tenant_url, auth_file = auth_options
                command.extend(["--host", tenant_url, "--options-file", auth_file])
            command.extend(args)
            # Windows commonly resolves npm-installed CLI commands to a .cmd
            # shim. Such shims cannot be launched with shell=False. Let cmd.exe
            # execute only the installed wrapper; arguments are escaped by
            # Python's Windows command-line quoting routine.
            use_windows_shell = os.name == "nt" and (
                str(command[0]).lower().endswith((".cmd", ".bat"))
                or command[0] == "datasphere"
            )
            result = subprocess.run(
                command,
                check=False,
                capture_output=True,
                text=True,
                timeout=60,
                shell=use_windows_shell,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise RuntimeError(f"Datasphere CLI invocation failed: {exc}") from exc
        finally:
            if auth_file:
                try:
                    Path(auth_file).unlink()
                except OSError:
                    logger.warning("Could not remove temporary Datasphere CLI auth options file.")
        if result.returncode:
            detail = result.stderr.strip() or result.stdout.strip()
            raise RuntimeError(
                f"Datasphere CLI exited with status {result.returncode}: "
                f"{detail[:1000]}"
            )
        return self._decode_cli_json(result.stdout)

    def _read_cli_object(self, object_type: str, object_name: str, space: str | None = None) -> dict:
        artifact_type = CLI_OBJECT_TYPES[object_type]
        target_space = (space or self.space).strip()
        if not CLI_IDENTIFIER.fullmatch(target_space) or not CLI_IDENTIFIER.fullmatch(object_name):
            raise ValueError("Invalid Datasphere space or object technical name.")

        cache_key = (target_space.casefold(), object_type.casefold(), object_name.casefold())
        if cache_key in self._definition_cache:
            self.cli_reads.add((object_type, object_name.casefold()))
            return self._definition_cache[cache_key]

        # Use specific accept content type based on object type
        primary_accept = self.accept_content_type
        fallback_accept = None
        if object_type == "analytic_model":
            primary_accept = "application/vnd.sap.datasphere.object.content.simplified+json"
            fallback_accept = self.accept_content_type
        elif object_type in ("view", "local_table", "fact_model"):
            primary_accept = "application/vnd.sap.datasphere.object.content.design-time+json"
            fallback_accept = self.accept_content_type

        try:
            payload = self._run_cli([
                "objects", artifact_type, "read",
                "--space", target_space,
                "--technical-name", object_name,
                "--accept", primary_accept,
            ])
        except Exception:
            if fallback_accept and fallback_accept != primary_accept:
                payload = self._run_cli([
                    "objects", artifact_type, "read",
                    "--space", target_space,
                    "--technical-name", object_name,
                    "--accept", fallback_accept,
                ])
            else:
                raise

        definition = self._unwrap_cli_definition(payload)
        self._definition_cache[cache_key] = definition
        self.cli_reads.add((object_type, object_name.casefold()))
        return definition

    def _list_cli_objects(self, object_type: str, space: str, top: int) -> list[dict]:
        if not self.cli_available:
            return []
        if not CLI_IDENTIFIER.fullmatch(space):
            raise ValueError("Invalid Datasphere space technical name.")
        if top < 1:
            raise ValueError("top must be a positive integer.")
        payload = self._run_cli([
            "objects", CLI_OBJECT_TYPES[object_type], "list",
            "--space", space,
            "--top", str(top),
        ])
        return self._unwrap_cli_list(payload)

    def _sync_from_cache(self, space: str):
        """Load cached inventory names only; never synthesize object definitions.

        Cache entries are useful for discovery fallback, but they are not proof
        of current live lineage. Definition reads must come from the live CLI or
        from a genuinely stored CSN definition.
        """
        if not self.cache:
            return
        try:
            for item in self.cache.list_replication_local_tables():
                name = str(item.get("table_name") or "").strip()
                if name and name not in self.local_tables:
                    self.local_tables[name] = {
                        "_cache_inventory_only": True,
                        "name": name,
                        "space": os.getenv("DATASPHERE_REPLICATION_SPACE", ""),
                    }
        except Exception as exc:
            logger.debug("Replication cache inventory unavailable: %s", exc)
        try:
            for item in self.cache.list_cached_models(space):
                name = str(item.get("model_name") or "").strip()
                definition = item.get("csn_definition") or item.get("definition")
                if name and isinstance(definition, dict) and definition:
                    self.models.setdefault(name, definition)
        except Exception as exc:
            logger.debug("Analytical-model cache inventory unavailable: %s", exc)

    # ----------------------------------------------------------------
    # Discovery list methods (top=500)
    # ----------------------------------------------------------------

    def list_analytic_models(self, space: str, top: int = 500) -> list[dict]:
        target_space = space or self.space
        if self.cli_available:
            return self._list_cli_objects("analytic_model", target_space, top)
        self._sync_from_cache(target_space)
        return [{"name": k, "technicalName": k} for k in self.models.keys()][:top]

    def list_views(self, space: str, top: int = 500) -> list[dict]:
        if self.cli_available:
            return self._list_cli_objects("view", space or self.space, top)
        return [{"name": k, "technicalName": k} for k in self.views.keys()][:top]

    def list_local_tables(self, space: str, top: int = 500) -> list[dict]:
        if self.cli_available:
            return self._list_cli_objects("local_table", space or self.space, top)
        return [{"name": k, "technicalName": k} for k in self.local_tables.keys()][:top]

    def refresh_replication_local_table_index(
        self,
        replication_space: str = REPLICATION_SPACE_ID,
        top: int = 5000,
    ) -> list[dict]:
        """Refresh the persistent configured replication-space local-table snapshot from the CLI."""
        if not self.cli_available:
            raise RuntimeError("SAP Datasphere CLI is required to refresh the configured replication-space local tables.")
        if self.cache is None:
            raise RuntimeError("The Datasphere local-table cache could not be initialized.")

        objects = self._list_cli_objects("local_table", replication_space, top)
        if not objects:
            raise RuntimeError(
                f"The Datasphere CLI returned no local tables for {replication_space}; "
                "the saved source index was not replaced."
            )
        if len(objects) >= top:
            raise RuntimeError(
                f"The {replication_space} local-table list reached its {top}-object limit; "
                "refusing to cache a potentially partial inventory."
            )

        inventory = []
        for item in objects:
            name = next(
                (
                    item.get(key)
                    for key in ("technicalName", "technical_name", "objectName", "name", "id")
                    if isinstance(item.get(key), str) and item[key].strip()
                ),
                None,
            )
            if not name:
                raise ValueError(
                    f"The Datasphere CLI returned a local-table item without a technical name: {item!r}"
                )
            source_candidates = self._source_name_candidates(item)
            inventory.append({
                "table_name": name.strip(),
                "cds_view_name": source_candidates[0] if source_candidates else name.strip(),
                "table_type": "local_table",
                "replicated_from_adt": bool(item.get("replicated_from_adt", False)),
                "description": item.get("description", ""),
                "payload": item,
            })

        self.cache.sync_space_inventory(
            replication_space,
            inventory,
            complete_snapshot=True,
        )
        return self.cache.list_replication_local_tables(replication_space)

    @staticmethod
    def _source_name_candidates(value: Any) -> list[str]:
        source_keys = {
            "sourceentity", "sourceobject", "remoteobject",
            "remotesourceentity", "sourcecdsview", "cdsviewname",
            "sourcename", "remoteobjectname", "dataentity", "datasource",
        }
        name_keys = {"name", "technicalname", "entityname", "objectname", "key"}
        candidates: list[str] = []

        def visit(node: Any) -> None:
            if isinstance(node, dict):
                for key, child in node.items():
                    normalized_key = re.sub(r"[^a-z]", "", str(key).lower())
                    if normalized_key in source_keys:
                        if isinstance(child, str) and child.strip():
                            candidates.append(child.strip())
                        elif isinstance(child, dict):
                            for nested_key, nested_value in child.items():
                                if (
                                    re.sub(r"[^a-z]", "", str(nested_key).lower())
                                    in name_keys
                                    and isinstance(nested_value, str)
                                    and nested_value.strip()
                                ):
                                    candidates.append(nested_value.strip())
                        elif isinstance(child, list):
                            visit(child)
                    visit(child)
            elif isinstance(node, list):
                for child in node:
                    visit(child)

        visit(value)
        return list(dict.fromkeys(candidates))

    def match_dependency_sources(
        self,
        dependency_tree: dict,
        replicated_tables: list[dict] | None = None,
    ) -> list[dict]:
        """Match dependency names exactly against the configured replication-space local-table snapshot."""
        if self.cache is None:
            raise RuntimeError("The Datasphere local-table cache could not be initialized.")
        if replicated_tables is None:
            replicated_tables = self.cache.list_replication_local_tables()

        table_index = {
            str(table.get("table_name") or "").strip().casefold(): table
            for table in replicated_tables
            if table.get("table_name")
        }
        matches: list[dict] = []
        matched_names: set[str] = set()

        def visit(node: dict, parent_path: str = "") -> None:
            name = str(node.get("name") or "").strip()
            object_type = str(node.get("object_type") or "unknown")
            path = f"{parent_path} -> {name}" if parent_path and name else name or parent_path
            candidates = [name]
            definition = node.get("definition", {})
            if isinstance(definition, dict):
                candidates.extend(definition.get("source_names", []))
                candidates.extend(self._source_name_candidates(definition))
            metadata = node.get("metadata", {})
            if isinstance(metadata, dict):
                candidates.extend(self._source_name_candidates(metadata))

            if object_type != "analytic_model":
                for candidate_name in dict.fromkeys(
                    item.strip() for item in candidates
                    if isinstance(item, str) and item.strip()
                ):
                    candidate = candidate_name.rsplit(".", 1)[-1].casefold()
                    table = table_index.get(candidate)
                    if not table or candidate in matched_names:
                        continue
                    matched_names.add(candidate)
                    source_name = str(table.get("table_name") or candidate_name)
                    matches.append({
                        "name": candidate_name,
                        "type": object_type,
                        "lineage_path": path,
                        "matched_cds_view": source_name,
                        "replicated_from_adt": bool(table.get("replicated_from_adt", False)),
                        "replication_space": REPLICATION_SPACE_ID or None,
                        "description": table.get("description"),
                        "key_fields": table.get("key_fields", []),
                        "table_name": source_name,
                        "cds_view_name": source_name,
                        "matched_by": "exact_case_insensitive_name",
                        "source_origin_status": "verified" if table.get("replicated_from_adt") else "unverified",
                    })
            for child in node.get("dependencies", []):
                if isinstance(child, dict):
                    visit(child, path)

        if isinstance(dependency_tree, dict):
            visit(dependency_tree)
        return matches

    def list_fact_models(self, space: str, top: int = 500) -> list[dict]:
        if self.cli_available:
            return self._list_cli_objects("fact_model", space or self.space, top)
        return [{"name": k, "technicalName": k} for k in self.fact_models.keys()][:top]

    def list_remote_tables(self, space: str, top: int = 500) -> list[dict]:
        if self.cli_available:
            return self._list_cli_objects("remote_table", space or self.space, top)
        return self.remote_tables[:top]

    # ----------------------------------------------------------------
    # Detail reader methods
    # ----------------------------------------------------------------

    def read_analytic_model(self, space: str, object_name: str) -> dict:
        if self.cli_available:
            return self._read_cli_object("analytic_model", object_name, space)
        target_space = space or self.space
        self._sync_from_cache(target_space)

        for k, v in self.models.items():
            if k.upper() == object_name.upper():
                return v

        # Check cache directly for any dynamically entered model
        if self.cache:
            m = self.cache.get_cached_model(target_space, object_name)
            if m:
                self._sync_from_cache(target_space)
                for k, v in self.models.items():
                    if k.upper() == object_name.upper():
                        return v

        raise LookupError(
            f"No live Datasphere CLI definition or genuine cached CSN definition was found "
            f"for analytical model {object_name!r} in space {target_space!r}."
        )

    def read_view(self, space: str, object_name: str) -> dict:
        if self.cli_available:
            return self._read_cli_object("view", object_name, space)
        for k, v in self.views.items():
            if k.upper() == object_name.upper():
                return v
        raise LookupError(
            f"No live Datasphere CLI definition was found for view {object_name!r} "
            f"in space {(space or self.space)!r}."
        )

    def read_local_table(self, space: str, object_name: str) -> dict:
        if self.cli_available:
            return self._read_cli_object("local_table", object_name, space)
        for k, v in self.local_tables.items():
            if k.upper() == object_name.upper():
                return v
        raise LookupError(
            f"No live Datasphere CLI definition was found for local table {object_name!r} "
            f"in space {(space or self.space)!r}."
        )

    def read_fact_model(self, space: str, object_name: str) -> dict:
        if self.cli_available:
            return self._read_cli_object("fact_model", object_name, space)
        for k, v in self.fact_models.items():
            if k.upper() == object_name.upper():
                return v
        raise LookupError(
            f"No live Datasphere CLI definition was found for fact model {object_name!r} "
            f"in space {(space or self.space)!r}."
        )

    def read_remote_table(self, space: str, object_name: str) -> dict:
        if self.cli_available:
            return self._read_cli_object("remote_table", object_name, space)
        for entry in self.remote_tables:
            if str(entry.get("name", "")).casefold() == object_name.casefold():
                return entry
        raise LookupError(
            f"No live Datasphere CLI definition was found for remote table {object_name!r} "
            f"in space {(space or self.space)!r}."
        )

    def register_model(self, model_name: str, csn_definition: dict):
        """Allows dynamically registering any Analytical Model definition at runtime."""
        self.models[model_name] = csn_definition
        logger.info(f"Registered dynamic analytical model definition for: {model_name}")

    def register_view(self, view_name: str, csn_definition: dict):
        """Allows dynamically registering an underlying view definition at runtime."""
        self.views[view_name] = csn_definition
        logger.info(f"Registered dynamic view definition for: {view_name}")
