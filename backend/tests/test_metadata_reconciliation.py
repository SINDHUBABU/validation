import sys
import json
import os
os.environ.setdefault("DATASPHERE_REPLICATION_SPACE", "RAD_HDLF")
os.environ.setdefault("DATASPHERE_MODELING_SPACE", "RADH_S3X")
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from reconciliation.datasphere_resolver_client import DatasphereResolverClient
from reconciliation.dependency_resolver import DatasphereDependencyResolver
from reconciliation.datasphere_space_cache import DatasphereSpaceCache
from validation_service import (
    _validate_reconciliation_sql,
)


class MetadataReconciliationTests(unittest.TestCase):
    def test_datasphere_query_is_limited_to_selected_model(self):
        _validate_reconciliation_sql(
            'SELECT "CompanyCode", "Revenue" FROM "SALES"."AM_REVENUE"',
            expected_sources=[],
            datasphere_model="AM_REVENUE",
        )
        with self.assertRaises(ValueError):
            _validate_reconciliation_sql(
                'SELECT * FROM "SALES"."GV_RAW"',
                expected_sources=[],
                datasphere_model="AM_REVENUE",
            )

    def test_s4_query_requires_lineaged_sources_and_known_fields(self):
        _validate_reconciliation_sql(
            "SELECT S~SalesOrder AS SalesOrder FROM I_SALESORDER AS S WHERE S~Text = 'DELETE'",
            expected_sources=["I_SALESORDER"],
            allowed_source_fields=["SalesOrder", "Text"],
        )
        with self.assertRaises(ValueError):
            _validate_reconciliation_sql(
                "SELECT S~SalesOrder FROM I_OTHER AS S",
                expected_sources=["I_SALESORDER"],
                allowed_source_fields=["SalesOrder"],
            )
        with self.assertRaises(ValueError):
            _validate_reconciliation_sql(
                "SELECT S~UnknownField FROM I_SALESORDER AS S",
                expected_sources=["I_SALESORDER"],
                allowed_source_fields=["SalesOrder"],
            )

    def test_dependency_resolver_reads_remote_table_metadata(self):
        client = Mock()
        client.list_views.return_value = []
        client.list_local_tables.return_value = []
        client.list_analytic_models.return_value = []
        client.list_fact_models.return_value = []
        client.list_remote_tables.return_value = [{"name": "RT_SALES"}]
        client.read_remote_table.return_value = {
            "definitions": {
                "RT_SALES": {
                    "remoteObject": {"name": "I_SALESORDER"},
                }
            }
        }
        resolver = DatasphereDependencyResolver(client=client, space="SALES", silent=True)

        definition = resolver._read_definition("RT_SALES", "remote_table")

        self.assertEqual(
            definition["definitions"]["RT_SALES"]["remoteObject"]["name"],
            "I_SALESORDER",
        )
        client.read_remote_table.assert_called_once_with("SALES", "RT_SALES")

    def test_dependency_resolver_detects_selected_view_type(self):
        client = Mock()
        client.list_views.return_value = [{"name": "V_ORDERS"}]
        client.list_local_tables.return_value = []
        client.list_analytic_models.return_value = []
        client.list_fact_models.return_value = []
        client.list_remote_tables.return_value = []
        client.read_view.return_value = {
            "definitions": {"V_ORDERS": {"elements": {}}}
        }
        resolver = DatasphereDependencyResolver(client=client, space="SALES", silent=True)

        tree = resolver.resolve("V_ORDERS")

        self.assertEqual(tree["object_type"], "view")
        self.assertEqual(tree["status"], "resolved")
        client.read_view.assert_called_once_with("SALES", "V_ORDERS")

    def test_cli_reader_uses_artifact_type_and_json_accept_header(self):
        cache = Mock()
        cache.list_replication_local_tables.return_value = []
        cache.list_cached_models.return_value = []
        client = DatasphereResolverClient(space="SALES", cache=cache)
        client.cli_available = True
        client._run_cli = Mock(return_value={"definitions": {"V_ORDERS": {"kind": "entity"}}})

        definition = client.read_view("SALES", "V_ORDERS")

        self.assertIn("definitions", definition)
        client._run_cli.assert_called_once_with([
            "objects", "views", "read",
            "--space", "SALES",
            "--technical-name", "V_ORDERS",
            "--accept", "application/vnd.sap.datasphere.object.content.design-time+json",
        ])
        self.assertIn(("view", "v_orders"), client.cli_reads)

    def test_cli_uses_ephemeral_client_credentials_options(self):
        cache = Mock()
        cache.list_replication_local_tables.return_value = []
        cache.list_cached_models.return_value = []
        client = DatasphereResolverClient(space="SALES", cache=cache)
        client.cli_available = True
        result = Mock(returncode=0, stdout='{"ok": true}', stderr="")
        command_options = {}

        def run_cli(command, **kwargs):
            command_options["command"] = command
            options_path = Path(command[command.index("--options-file") + 1])
            command_options["options_path"] = options_path
            command_options["options"] = json.loads(options_path.read_text(encoding="utf-8"))
            return result

        environment = {
            "DATASPHERE_API_URL": "https://tenant.example.test/api/v1/data",
            "DATASPHERE_TOKEN_URL": "https://auth.example.test/oauth/token",
            "DATASPHERE_AUTHORIZATION_URL": "",
            "OAUTH_CLIENT_ID": "test-client-id",
            "OAUTH_CLIENT_SECRET": "test-client-secret",
        }
        with patch.dict(os.environ, environment), patch(
            "reconciliation.datasphere_resolver_client.subprocess.run",
            side_effect=run_cli,
        ):
            payload = client._run_cli(["objects", "views", "list", "--space", "SALES"])

        self.assertEqual(payload, {"ok": True})
        self.assertEqual(
            command_options["options"],
            {
                "client-id": "test-client-id",
                "client-secret": "test-client-secret",
                "token-url": environment["DATASPHERE_TOKEN_URL"],
                "authorization-url": "https://auth.example.test/oauth/authorize",
                "authorization-flow": "client_credentials",
            },
        )
        self.assertNotIn("test-client-secret", command_options["command"])
        self.assertFalse(command_options["options_path"].exists())
        self.assertEqual(
            command_options["command"][1:5],
            [
                "--host",
                "https://tenant.example.test",
                "--options-file",
                str(command_options["options_path"]),
            ],
        )

    def test_replicated_local_table_inventory_persists_and_matches_exactly(self):
        with tempfile.TemporaryDirectory() as directory:
            database_path = Path(directory) / "datasphere-cache.sqlite3"
            first_cache = DatasphereSpaceCache(database_path)
            first_cache.sync_space_inventory(
                "RAD_HDLF",
                [{
                    "table_name": "I_SALESORDER",
                    "cds_view_name": "I_SALESORDER",
                    "table_type": "local_table",
                    "replicated_from_adt": True,
                }],
                complete_snapshot=True,
            )

            reopened_cache = DatasphereSpaceCache(database_path)
            self.assertEqual(
                [table["table_name"] for table in reopened_cache.list_replication_local_tables()],
                ["I_SALESORDER"],
            )
            matches = reopened_cache.match_replication_local_tables(
                ["namespace.I_salesorder", "I_SALESORDER_EXTRA", "LT_I_SALESORDER"]
            )
            first_cache.close()
            reopened_cache.close()

        self.assertEqual([match["table_name"] for match in matches], ["I_SALESORDER"])
        self.assertTrue(matches[0]["verified_in_replication_space"])

    def test_client_refreshes_and_matches_dependency_against_rad_hdlf(self):
        with tempfile.TemporaryDirectory() as directory:
            cache = DatasphereSpaceCache(Path(directory) / "cache.sqlite3")
            client = DatasphereResolverClient(space="RADH_S3X", cache=cache)
            client.cli_available = True
            client._list_cli_objects = Mock(
                return_value=[{"technicalName": "I_SALESORDER"}]
            )

            inventory = client.refresh_replication_local_table_index()
            matches = client.match_dependency_sources({
                "name": "AM_REVENUE",
                "object_type": "analytic_model",
                "dependencies": [{
                    "name": "V_ORDERS",
                    "object_type": "view",
                    "dependencies": [{
                        "name": "I_SALESORDER",
                        "object_type": "local_table",
                        "dependencies": [],
                    }],
                }],
            }, inventory)
            cache.close()

        self.assertEqual(len(inventory), 1)
        self.assertEqual(len(matches), 1)
        self.assertEqual(matches[0]["matched_cds_view"], "I_SALESORDER")
        self.assertEqual(matches[0]["replication_space"], "RAD_HDLF")
        self.assertEqual(matches[0]["matched_by"], "exact_case_insensitive_name")

    def test_client_matches_cds_name_embedded_in_remote_table_metadata(self):
        cache = Mock()
        cache.list_replication_local_tables.return_value = []
        client = DatasphereResolverClient(space="RADH_S3X", cache=cache)
        tree = {
            "name": "AM_PURCHASEKPI",
            "object_type": "analytic_model",
            "dependencies": [{
                "name": "RT_PURCHASEKPI",
                "object_type": "remote_table",
                "definition": {
                    "definitions": {
                        "RT_PURCHASEKPI": {
                            "remoteObject": {"name": "ZI_PURCHASEKPIBASE"},
                        },
                    },
                },
                "dependencies": [],
            }],
        }
        replicated_tables = [{
            "table_name": "zi_purchasekpibase",
            "cds_view_name": "zi_purchasekpibase",
            "description": "Replicated CDS view",
        }]

        matches = client.match_dependency_sources(tree, replicated_tables)

        self.assertEqual(len(matches), 1)
        self.assertEqual(matches[0]["matched_cds_view"], "zi_purchasekpibase")
        self.assertEqual(matches[0]["name"], "ZI_PURCHASEKPIBASE")
        self.assertEqual(matches[0]["type"], "remote_table")

    def test_querybuilder_normalization_preserves_source_references(self):
        definition = {
            "model": {
                "classDefinition": "sap.cdw.querybuilder.Element",
                "name": "Warning",
                "expression": "CASE WHEN Amount > 0 THEN 1 ELSE 0 END",
                "dataType": "cds.Integer",
            },
            "source": {
                "remoteObject": {"name": "ZI_PURCHASEKPIBASE"},
            },
        }

        normalized = DatasphereDependencyResolver._normalize_definition(
            definition,
            "AM_PURCHASEKPI",
        )

        self.assertEqual(normalized["source_names"], ["ZI_PURCHASEKPIBASE"])
        self.assertEqual(
            normalized["definitions"]["AM_PURCHASEKPI"]["elements"]["Warning"]["expression"],
            "CASE WHEN Amount > 0 THEN 1 ELSE 0 END",
        )

    def test_business_layer_calculated_measure_formula_is_in_field_catalog(self):
        resolver = DatasphereDependencyResolver(
            client=Mock(),
            space="RADH_S3X",
            silent=True,
        )
        details = {"field_catalog": []}

        resolver._extract_field_catalog({
            "calculatedMeasures": {
                "KPI_Breach": {
                    "formula": "CASE WHEN Warning > 0 THEN 1 ELSE 0 END",
                    "type": "cds.Integer",
                },
            },
        }, details)

        self.assertEqual(len(details["field_catalog"]), 1)
        self.assertEqual(details["field_catalog"][0]["name"], "KPI_Breach")
        self.assertEqual(
            details["field_catalog"][0]["calculation"],
            "CASE WHEN Warning > 0 THEN 1 ELSE 0 END",
        )

    def test_empty_rad_hdlf_refresh_does_not_delete_cached_inventory(self):
        with tempfile.TemporaryDirectory() as directory:
            cache = DatasphereSpaceCache(Path(directory) / "cache.sqlite3")
            cache.sync_space_inventory(
                "RAD_HDLF",
                [{
                    "table_name": "I_SALESORDER",
                    "cds_view_name": "I_SALESORDER",
                    "table_type": "local_table",
                }],
                complete_snapshot=True,
            )
            client = DatasphereResolverClient(space="RADH_S3X", cache=cache)
            client.cli_available = True
            client._list_cli_objects = Mock(return_value=[])

            with self.assertRaisesRegex(RuntimeError, "returned no local tables"):
                client.refresh_replication_local_table_index()

            self.assertEqual(
                [table["table_name"] for table in cache.list_replication_local_tables()],
                ["I_SALESORDER"],
            )
            cache.close()


if __name__ == "__main__":
    unittest.main()
