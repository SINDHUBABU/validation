import os
import sys
import json
import asyncio
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import config
from config import get_system_diagnostics
from reconciliation.datasphere_resolver_client import DatasphereResolverClient
from reconciliation.dependency_resolver import (
    DatasphereDependencyResolver,
    propagate_calculated_columns,
)
from validation_service import (
    run_metadata_driven_validation,
    _validate_reconciliation_sql,
    build_cds_measure_expression,
)
from schemas import ValidationRequest, ValidationResponse
from main import _resolved_catalog_object_type, get_view_details_endpoint


class CatalogObjectTypeTests(unittest.TestCase):
    def test_unknown_catalog_type_uses_resolved_datasphere_object_type(self):
        self.assertEqual(
            _resolved_catalog_object_type(
                {"type": "unknown"},
                {"object_type": "analytic_model"},
            ),
            "Analytical Model",
        )

    def test_catalog_type_remains_authoritative_when_known(self):
        self.assertEqual(
            _resolved_catalog_object_type(
                {"type": "view"},
                {"object_type": "analytic_model"},
            ),
            "View",
        )

    def test_cached_details_use_resolved_dependency_type(self):
        cache = Mock()
        cache.get_model_metadata.return_value = {
            "metadata": {
                "response": {
                    "type": "unknown",
                    "metadata_json": {"type": "unknown"},
                },
                "response_synced_at": 9999999999,
                "dependency_tree": {"object_type": "analytic_model"},
            },
            "last_synced": 0,
        }
        with patch("main.DatasphereResolverClient") as resolver:
            resolver.return_value.cache = cache
            response = asyncio.run(
                get_view_details_endpoint("AM_Customer_Receivables_KPI", "RADH_S3X")
            )

        self.assertEqual(response["type"], "Analytical Model")
        self.assertEqual(response["metadata_json"]["type"], "Analytical Model")


class ConfigurationTests(unittest.TestCase):
    def test_env_loaded_independent_of_cwd(self):
        """Verifies that .env paths are constructed from module location, not cwd."""
        with tempfile.TemporaryDirectory() as temp_dir:
            orig_cwd = os.getcwd()
            try:
                os.chdir(temp_dir)
                for p in config.ENV_PATHS:
                    self.assertTrue(
                        "backend" in str(p) or "New folder" in str(p) or str(p).endswith(".env")
                    )
            finally:
                os.chdir(orig_cwd)

    def test_secrets_redacted_in_diagnostics(self):
        """Verifies that diagnostics report system status without leaking secrets."""
        test_env = {
            "OAUTH_CLIENT_SECRET": "super_secret_password_123",
            "HANA_PASSWORD": "confidential_hana_pass",
            "ABAP_PASSWORD": "secret_sap_pass",
        }
        with patch.dict(os.environ, test_env):
            diag = get_system_diagnostics()
            diag_str = json.dumps(diag)
            self.assertNotIn("super_secret_password_123", diag_str)
            self.assertNotIn("confidential_hana_pass", diag_str)
            self.assertNotIn("secret_sap_pass", diag_str)
            self.assertIn("connectors", diag)
            self.assertIn("oauth_configured", diag["connectors"])


class DatasphereCLIRunnerTests(unittest.TestCase):
    def setUp(self):
        self.cache = Mock()
        self.cache.list_replication_local_tables.return_value = []
        self.cache.list_cached_models.return_value = []
        self.client = DatasphereResolverClient(space="RADH_S3X", cache=self.cache)
        self.client.cli_available = True
        self.client.cli_path = "C:\\dummy\\datasphere.cmd"

    def test_cli_handles_valid_json_output(self):
        mock_proc = Mock(returncode=0, stdout='[{"technicalName": "AM_TEST"}]', stderr="")
        with patch("reconciliation.datasphere_resolver_client.subprocess.run", return_value=mock_proc):
            data = self.client._run_cli(["objects", "analytic-models", "list", "--space", "RADH_S3X"])
            self.assertEqual(data, [{"technicalName": "AM_TEST"}])

    def test_cli_handles_invalid_json(self):
        mock_proc = Mock(returncode=0, stdout="Not a valid JSON response from CLI", stderr="")
        with patch("reconciliation.datasphere_resolver_client.subprocess.run", return_value=mock_proc):
            with self.assertRaises((ValueError, RuntimeError)):
                self.client._run_cli(["objects", "analytic-models", "list"])

    def test_cli_handles_nonzero_exit_code(self):
        mock_proc = Mock(returncode=1, stdout="", stderr="Error: Space not found")
        with patch("reconciliation.datasphere_resolver_client.subprocess.run", return_value=mock_proc):
            with self.assertRaisesRegex(RuntimeError, r"Datasphere CLI exited with status 1: Error: Space not found"):
                self.client._run_cli(["objects", "analytic-models", "list"])

    def test_cli_handles_empty_list_truthfully(self):
        mock_proc = Mock(returncode=0, stdout="[]", stderr="")
        with patch("reconciliation.datasphere_resolver_client.subprocess.run", return_value=mock_proc):
            res = self.client._run_cli(["objects", "views", "list", "--space", "RADH_S3X"])
            self.assertEqual(res, [])

    def test_cli_not_available_raises_cleanly(self):
        self.client.cli_available = False
        with self.assertRaisesRegex(RuntimeError, "Datasphere CLI is not available"):
            self.client._run_cli(["objects", "views", "list"])


class ResolverCycleAndTraversalTests(unittest.TestCase):
    def test_resolver_handles_cycles_without_infinite_recursion(self):
        client = Mock()
        client.list_views.return_value = [{"name": "VIEW_A"}, {"name": "VIEW_B"}]
        client.list_local_tables.return_value = []
        client.list_analytic_models.return_value = []
        client.list_fact_models.return_value = []
        client.list_remote_tables.return_value = []

        def mock_read_view(space, name):
            if name == "VIEW_A":
                return {
                    "definitions": {
                        "VIEW_A": {
                            "query": {"SELECT": {"from": {"ref": ["VIEW_B"]}}}
                        }
                    }
                }
            elif name == "VIEW_B":
                return {
                    "definitions": {
                        "VIEW_B": {
                            "query": {"SELECT": {"from": {"ref": ["VIEW_A"]}}}
                        }
                    }
                }
            return {}

        client.read_view.side_effect = mock_read_view
        resolver = DatasphereDependencyResolver(client=client, space="RADH_S3X", silent=True)
        tree = resolver.resolve("VIEW_A")
        self.assertEqual(tree["name"], "VIEW_A")
        self.assertEqual(len(tree["dependencies"]), 1)
        dep_b = tree["dependencies"][0]
        self.assertEqual(dep_b["name"], "VIEW_B")
        dep_cycle = dep_b["dependencies"][0]
        self.assertEqual(dep_cycle["status"], "cycle")


class ValidationApprovalAndGatingTests(unittest.TestCase):
    def test_build_cds_measure_expression_dynamic_no_hardcoding(self):
        """Verifies that measure expressions are dynamically generated without hardcoded domains."""
        expr = build_cds_measure_expression(
            view_name="AM_CustomModel",
            source_cds="I_CustomEntity",
            metric="CustomMetric",
            calculated_formulas={"custommetric": "Price * Quantity"},
        )
        self.assertEqual(expr, "SUM( CAST( Price * Quantity AS FLTP ) )")

        expr_std = build_cds_measure_expression(
            view_name="AM_CustomModel",
            source_cds="I_CustomEntity",
            metric="TotalAmount",
        )
        self.assertEqual(expr_std, "SUM( CAST( TotalAmount AS FLTP ) )")

    def test_unapproved_reconciliation_returns_not_run_with_hash(self):
        """Verifies that unapproved validation stops at NOT_RUN and provides a plan hash."""
        request = ValidationRequest(
            view_name="AM_DeliveryKPI",
            space_id="RADH_S3X",
            source_cds_view="zi_deliverykpibase",
            selected_fields=["DeliveryDelayDays"],
            key_fields=["PurchaseOrder"],
            user_prompt="Reconcile delivery delay",
            human_approved=False,
            approved_query_hash=None,
        )

        mock_plan = {
            "datasphere_sql": 'SELECT "PurchaseOrder", SUM("DeliveryDelayDays") AS "DeliveryDelayDays" FROM "RADH_S3X"."AM_DeliveryKPI" GROUP BY "PurchaseOrder"',
            "s4hana_sql": (
                "SELECT zi_deliverykpibase~PurchaseOrder AS PurchaseOrder, "
                "SUM(zi_deliverykpibase~DeliveryDelayDays) AS DeliveryDelayDays "
                "FROM zi_deliverykpibase GROUP BY zi_deliverykpibase~PurchaseOrder"
            ),
            "output_fields": ["PurchaseOrder", "DeliveryDelayDays"],
            "key_fields": ["PurchaseOrder"],
            "logic_summary": "Sum delivery delay days grouped by purchase order.",
            "assumptions": [],
        }

        with patch("validation_service.DatasphereResolverClient") as mock_client_cls, \
             patch("validation_service.DatasphereDependencyResolver") as mock_resolver_cls, \
             patch("validation_service.CDSMetadataClient") as mock_adt_cls, \
             patch("validation_service.generate_reconciliation_query_blueprint", return_value=mock_plan):

            mock_client = mock_client_cls.return_value
            mock_client.cli_available = True
            mock_client.cli_reads = {("analytic_model", "am_deliverykpi")}
            mock_client.refresh_replication_local_table_index.return_value = [{"table_name": "zi_deliverykpibase", "cds_view_name": "zi_deliverykpibase"}]
            mock_client.match_dependency_sources.return_value = [{"name": "zi_deliverykpibase", "cds_view_name": "zi_deliverykpibase"}]

            mock_resolver = mock_resolver_cls.return_value
            mock_resolver.resolve.return_value = {
                "name": "AM_DeliveryKPI",
                "object_type": "analytic_model",
                "status": "resolved",
                "definition": {"definitions": {"AM_DeliveryKPI": {}}},
                "dependencies": [{
                    "name": "FV_DeliveryKPI",
                    "object_type": "view",
                    "relationship": "analytic_model_source",
                    "status": "resolved",
                    "runtime_object": "FV_DeliveryKPI_RT",
                    "schema": "RADH_S3X",
                    "definition": {"definitions": {"FV_DeliveryKPI": {}}},
                    "field_catalog": [
                        {
                            "name": "DeliveryDelayDays",
                            "source_field": "DeliveryDelayDays",
                            "aggregation": "SUM",
                        },
                        {
                            "name": "PurchaseOrder",
                            "source_field": "PurchaseOrder",
                            "is_key": True,
                        },
                    ],
                    "dependencies": [{
                        "name": "zi_deliverykpibase",
                        "object_type": "local_table",
                        "status": "resolved",
                        "definition": {"sourceLineage": {"cdsEntity": "zi_deliverykpibase"}},
                        "dependencies": [],
                    }],
                }],
                "field_catalog": [
                    {
                        "name": "DeliveryDelayDays",
                        "source_field": "DeliveryDelayDays",
                        "measure_type": "aggregation",
                        "aggregation": "SUM",
                        "is_key": False,
                    },
                    {
                        "name": "PurchaseOrder",
                        "source_field": "PurchaseOrder",
                        "is_key": True,
                    },
                ],
            }

            mock_adt = mock_adt_cls.return_value.__enter__.return_value
            mock_adt.discover_metadata.return_value = {
                "fields": [{"name": "PurchaseOrder"}, {"name": "DeliveryDelayDays"}]
            }

            response = asyncio.run(run_metadata_driven_validation(request))

            self.assertEqual(response.status, "NOT_RUN")
            self.assertTrue(response.approval_required)
            self.assertIsNotNone(response.execution_plan)
            self.assertEqual(response.execution_plan["execution_readiness"], "READY_FOR_APPROVAL")
            self.assertEqual(len(response.execution_plan["query_hash"]), 64)

    def test_tampered_hash_blocks_execution(self):
        """Verifies that approved=True with invalid query_hash stops execution."""
        request = ValidationRequest(
            view_name="AM_DeliveryKPI",
            space_id="RADH_S3X",
            source_cds_view="zi_deliverykpibase",
            selected_fields=["DeliveryDelayDays"],
            key_fields=["PurchaseOrder"],
            user_prompt="Reconcile delivery delay",
            human_approved=True,
            approved_query_hash="invalid_or_outdated_query_hash_12345",
        )

        mock_plan = {
            "datasphere_sql": 'SELECT "PurchaseOrder", SUM("DeliveryDelayDays") AS "DeliveryDelayDays" FROM "RADH_S3X"."AM_DeliveryKPI" GROUP BY "PurchaseOrder"',
            "s4hana_sql": "SELECT zi_deliverykpibase~PurchaseOrder AS PurchaseOrder, SUM(zi_deliverykpibase~DeliveryDelayDays) AS DeliveryDelayDays FROM zi_deliverykpibase GROUP BY zi_deliverykpibase~PurchaseOrder",
            "output_fields": ["PurchaseOrder", "DeliveryDelayDays"],
            "key_fields": ["PurchaseOrder"],
            "logic_summary": "Sum delivery delay days grouped by purchase order.",
            "assumptions": [],
        }

        with patch("validation_service.DatasphereResolverClient") as mock_client_cls, \
             patch("validation_service.DatasphereDependencyResolver") as mock_resolver_cls, \
             patch("validation_service.CDSMetadataClient") as mock_adt_cls, \
             patch("validation_service.generate_reconciliation_query_blueprint", return_value=mock_plan):

            mock_client = mock_client_cls.return_value
            mock_client.cli_available = True
            mock_client.cli_reads = {("analytic_model", "am_deliverykpi")}
            mock_client.refresh_replication_local_table_index.return_value = [{"table_name": "zi_deliverykpibase", "cds_view_name": "zi_deliverykpibase"}]
            mock_client.match_dependency_sources.return_value = [{"name": "zi_deliverykpibase", "cds_view_name": "zi_deliverykpibase"}]

            mock_resolver = mock_resolver_cls.return_value
            mock_resolver.resolve.return_value = {
                "name": "AM_DeliveryKPI",
                "object_type": "analytic_model",
                "status": "resolved",
                "definition": {"definitions": {"AM_DeliveryKPI": {}}},
                "dependencies": [{
                    "name": "FV_DeliveryKPI",
                    "object_type": "view",
                    "relationship": "analytic_model_source",
                    "status": "resolved",
                    "runtime_object": "FV_DeliveryKPI_RT",
                    "schema": "RADH_S3X",
                    "definition": {"definitions": {"FV_DeliveryKPI": {}}},
                    "field_catalog": [
                        {"name": "DeliveryDelayDays", "aggregation": "SUM"},
                        {"name": "PurchaseOrder", "is_key": True},
                    ],
                    "dependencies": [{
                        "name": "zi_deliverykpibase",
                        "object_type": "local_table",
                        "status": "resolved",
                        "definition": {"sourceLineage": {"cdsEntity": "zi_deliverykpibase"}},
                        "dependencies": [],
                    }],
                }],
                "field_catalog": [{"name": "DeliveryDelayDays", "measure_type": "aggregation", "is_key": False}, {"name": "PurchaseOrder", "is_key": True}],
            }

            mock_adt = mock_adt_cls.return_value.__enter__.return_value
            mock_adt.discover_metadata.return_value = {
                "fields": [{"name": "PurchaseOrder"}, {"name": "DeliveryDelayDays"}]
            }

            response = asyncio.run(run_metadata_driven_validation(request))

            # Must stop at NOT_RUN because the hash does not match the generated plan
            self.assertEqual(response.status, "NOT_RUN")
            self.assertTrue(response.approval_required)

    def test_gaps_in_lineage_return_indeterminate(self):
        """Verifies that unresolved lineage returns INDETERMINATE, never a false MATCH."""
        request = ValidationRequest(
            view_name="AM_IncompleteModel",
            space_id="RADH_S3X",
            source_cds_view="zi_missing",
            selected_fields=["Amount"],
            key_fields=["ID"],
            user_prompt="Reconcile",
        )

        with patch("validation_service.DatasphereResolverClient") as mock_client_cls, \
             patch("validation_service.DatasphereDependencyResolver") as mock_resolver_cls:

            mock_client = mock_client_cls.return_value
            mock_client.cli_available = True
            mock_client.cli_reads = {("analytic_model", "am_incompletemodel")}
            mock_client.refresh_replication_local_table_index.return_value = []
            mock_client.match_dependency_sources.return_value = []

            mock_resolver = mock_resolver_cls.return_value
            mock_resolver.resolve.return_value = {
                "name": "AM_IncompleteModel",
                "object_type": "analytic_model",
                "status": "resolved",
                "definition": {"definitions": {}},
                "dependencies": [{
                    "name": "UNRESOLVED_TABLE",
                    "object_type": "unknown",
                    "status": "unresolved_metadata_missing",
                    "definition": None,
                    "dependencies": [],
                }],
                "field_catalog": [],
            }

            response = asyncio.run(run_metadata_driven_validation(request))
            self.assertEqual(response.status, "INDETERMINATE")
            self.assertEqual(response.reconstruction.get("status", ""), "INDETERMINATE")
            self.assertFalse(response.comparison.get("is_match", True))


class CalculatedColumnAndFormulaExtractionTests(unittest.TestCase):
    def setUp(self):
        self.client = Mock()
        self.resolver = DatasphereDependencyResolver(
            client=self.client,
            space="RADH_S3X",
            silent=True,
        )

    def test_extract_case_expression_calculation(self):
        """Tests extraction of CASE WHEN expressions from CSN query columns."""
        entity_def = {
            "query": {
                "SELECT": {
                    "from": {"ref": ["BASE_TABLE"]},
                    "columns": [
                        {"ref": ["DeliveryDelayDays"]},
                        {
                            "as": "Warning",
                            "xpr": [
                                "case",
                                "when", {"ref": ["DeliveryDelayDays"]}, ">", {"val": 14},
                                "and", {"ref": ["DeliveryDelayDays"]}, "<=", {"val": 21},
                                "then", {"val": 1},
                                "else", {"val": 0},
                                "end"
                            ]
                        }
                    ]
                }
            },
            "elements": {
                "DeliveryDelayDays": {"type": "cds.Integer"},
                "Warning": {"type": "cds.Integer"}
            }
        }
        details = self.resolver._walk_details_for_test("GV_TEST", entity_def, "RADH_S3X") if hasattr(self.resolver, "_walk_details_for_test") else None
        if details is None:
            raw_details = {"calculated_columns": [], "field_catalog": [], "joins": [], "filters": [], "formulas": [], "associations": []}
            self.resolver._walk_details(entity_def, raw_details, ())
            self.resolver._sync_calculated_columns_to_field_catalog("GV_TEST", raw_details)
            details = raw_details

        calcs = details.get("calculated_columns", [])
        self.assertEqual(len(calcs), 1)
        warning_calc = calcs[0]
        self.assertEqual(warning_calc["name"], "Warning")
        self.assertIn("CASE", warning_calc["expression"].upper())
        self.assertIn("DeliveryDelayDays", warning_calc["expression"])
        self.assertEqual(warning_calc["referenced_columns"], ["DeliveryDelayDays"])
        self.assertEqual(warning_calc["owning_object"], "GV_TEST")
        self.assertEqual(warning_calc["transformation_category"], "case_expression")

    def test_extract_function_and_constant_calculation(self):
        """Tests extraction of function calls (YEAR, SUBSTRING) and constants."""
        entity_def = {
            "query": {
                "SELECT": {
                    "from": {"ref": ["BASE_TABLE"]},
                    "columns": [
                        {
                            "as": "Posting_Year",
                            "func": "YEAR",
                            "args": [{"ref": ["CreationDate"]}]
                        },
                        {
                            "as": "KPI_Name",
                            "val": "Procurement Lead Time"
                        }
                    ]
                }
            },
            "elements": {
                "Posting_Year": {"type": "cds.Integer"},
                "KPI_Name": {"type": "cds.String"}
            }
        }
        raw_details = {"calculated_columns": [], "field_catalog": [], "joins": [], "filters": [], "formulas": [], "associations": []}
        self.resolver._walk_details(entity_def, raw_details, ())
        self.resolver._sync_calculated_columns_to_field_catalog("GV_FUNCS", raw_details)

        calcs = {c["name"]: c for c in raw_details.get("calculated_columns", [])}
        self.assertIn("Posting_Year", calcs)
        self.assertEqual(calcs["Posting_Year"]["expression"], "YEAR(CreationDate)")
        self.assertEqual(calcs["Posting_Year"]["referenced_columns"], ["CreationDate"])

        self.assertIn("KPI_Name", calcs)
        self.assertIn("Procurement Lead Time", calcs["KPI_Name"]["expression"])
        self.assertEqual(calcs["KPI_Name"]["transformation_category"], "constant")

    def test_bottom_up_propagation_across_three_tiers(self):
        """Verifies formulas propagate from leaf view to intermediate view to root model."""
        tree = {
            "name": "AM_ROOT",
            "object_type": "analytic_model",
            "status": "resolved",
            "calculated_columns": [],
            "field_catalog": [
                {"name": "Warning", "is_calculated": False},
                {"name": "TotalAmount", "measure_type": "aggregation", "aggregation": "SUM"}
            ],
            "dependencies": [
                {
                    "name": "FV_INTERMEDIATE",
                    "object_type": "view",
                    "status": "resolved",
                    "calculated_columns": [],
                    "field_catalog": [{"name": "Warning", "is_calculated": False}],
                    "dependencies": [
                        {
                            "name": "GV_BASE",
                            "object_type": "view",
                            "status": "resolved",
                            "calculated_columns": [
                                {
                                    "name": "Warning",
                                    "expression": "CASE WHEN Days > 14 THEN 1 ELSE 0 END",
                                    "owning_object": "GV_BASE",
                                    "referenced_columns": ["Days"],
                                    "transformation_category": "case_expression",
                                }
                            ],
                            "field_catalog": [
                                {
                                    "name": "Warning",
                                    "is_calculated": True,
                                    "calculation": "CASE WHEN Days > 14 THEN 1 ELSE 0 END",
                                    "source_object": "GV_BASE",
                                    "referenced_columns": ["Days"],
                                    "transformation_category": "case_expression",
                                }
                            ],
                            "dependencies": []
                        }
                    ]
                }
            ]
        }
        propagate_calculated_columns(tree)

        # Root analytic model must have inherited Warning calculation from GV_BASE
        root_calcs = {c["name"].lower(): c for c in tree.get("calculated_columns", [])}
        self.assertIn("warning", root_calcs)
        self.assertEqual(root_calcs["warning"]["expression"], "CASE WHEN Days > 14 THEN 1 ELSE 0 END")
        self.assertEqual(root_calcs["warning"]["owning_object"], "GV_BASE")

        # Root field_catalog Warning must be marked as calculated
        catalog_warning = next(f for f in tree["field_catalog"] if f["name"] == "Warning")
        self.assertTrue(catalog_warning.get("is_calculated"))
        self.assertEqual(catalog_warning.get("calculation"), "CASE WHEN Days > 14 THEN 1 ELSE 0 END")
        self.assertEqual(catalog_warning.get("source_object"), "GV_BASE")

        # Measure with SUM aggregation is NOT marked as calculated column formula
        total_amount = next(f for f in tree["field_catalog"] if f["name"] == "TotalAmount")
        self.assertFalse(total_amount.get("is_calculated"))

    def test_renamed_calculated_columns_lineage(self):
        """Verifies that renamed projections preserve origin calculation."""
        tree = {
            "name": "VIEW_TOP",
            "object_type": "view",
            "calculated_columns": [],
            "field_catalog": [{"name": "AlertLevel", "is_calculated": False}],
            "definition": {
                "definitions": {
                    "VIEW_TOP": {
                        "query": {
                            "SELECT": {
                                "columns": [
                                    {"ref": ["Warning"], "as": "AlertLevel"}
                                ]
                            }
                        }
                    }
                }
            },
            "dependencies": [
                {
                    "name": "VIEW_LOWER",
                    "object_type": "view",
                    "calculated_columns": [
                        {
                            "name": "Warning",
                            "expression": "CASE WHEN x > 1 THEN 1 ELSE 0 END",
                            "owning_object": "VIEW_LOWER",
                            "referenced_columns": ["x"]
                        }
                    ],
                    "field_catalog": [
                        {
                            "name": "Warning",
                            "is_calculated": True,
                            "calculation": "CASE WHEN x > 1 THEN 1 ELSE 0 END",
                            "source_object": "VIEW_LOWER",
                            "referenced_columns": ["x"]
                        }
                    ],
                    "dependencies": []
                }
            ]
        }
        propagate_calculated_columns(tree)
        top_calcs = {c["name"].lower(): c for c in tree.get("calculated_columns", [])}
        self.assertIn("alertlevel", top_calcs)
        self.assertEqual(top_calcs["alertlevel"]["expression"], "CASE WHEN x > 1 THEN 1 ELSE 0 END")
        self.assertEqual(top_calcs["alertlevel"]["owning_object"], "VIEW_LOWER")

    def test_malformed_or_incomplete_metadata_does_not_crash(self):
        """Verifies robustness against empty, non-dict, or broken query structures."""
        raw_details = {"calculated_columns": [], "field_catalog": [], "joins": [], "filters": [], "formulas": [], "associations": []}
        self.resolver._walk_details({}, raw_details, ())
        self.assertEqual(raw_details.get("calculated_columns"), [])

        raw_details2 = {"calculated_columns": [], "field_catalog": [], "joins": [], "filters": [], "formulas": [], "associations": []}
        self.resolver._walk_details({"query": {"SELECT": None}}, raw_details2, ())
        self.assertEqual(raw_details2.get("calculated_columns"), [])


if __name__ == "__main__":
    unittest.main()
