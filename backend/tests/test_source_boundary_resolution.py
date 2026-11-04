"""
Comprehensive unit tests for source boundary resolution, cross-space traversal,
calculated-column transformations, and independent validation integrity.
Enforces zero hardcoding and strict safety gates.
"""

import sys
import unittest
import asyncio
from pathlib import Path
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from reconciliation.dependency_resolver import DatasphereDependencyResolver
from validation_service import (
    _derive_model_keys,
    _validate_reconciliation_sql,
    _verify_transformation_coverage,
    _project_open_sql_grouping_expressions,
    run_metadata_driven_validation,
)
from llm_client import compile_reconciliation_blueprint_deterministic, _substitute_cds_fields
from schemas import ValidationRequest
import llm_client


class SourceBoundaryResolutionTests(unittest.TestCase):
    def setUp(self):
        self.mock_client = Mock()
        self.mock_client.space = "RADH_S3X"
        self.mock_client.list_views.return_value = []
        self.mock_client.list_local_tables.return_value = []
        self.mock_client.list_analytic_models.return_value = []
        self.mock_client.list_fact_models.return_value = []
        self.mock_client.list_remote_tables.return_value = []


    def test_dependency_graph_ending_in_shared_local_table_across_spaces(self):
        """
        Scenario 1: Analytical model in RADH_S3X references an intermediate view
        which references a shared local table residing in space RAD_S3X.
        Resolver must parse the cross-space reference, query RAD_S3X, and mark
        the terminal node as a shared local table boundary.
        """
        self.mock_client.list_analytic_models.return_value = [{"name": "AM_TEST_MODEL"}]
        self.mock_client.list_views.return_value = [{"name": "INTERMEDIATE_VIEW"}]
        self.mock_client.list_local_tables.return_value = [{"name": "SHARED_LOCAL_TABLE"}]

        self.mock_client.read_analytic_model.return_value = {
            "definitions": {
                "AM_TEST_MODEL": {
                    "query": {
                        "from": {"ref": ["INTERMEDIATE_VIEW"]},
                    },
                    "elements": {
                        "KPI_VAL": {"type": "cds.Integer"}
                    }
                }
            }
        }
        self.mock_client.read_view.return_value = {
            "definitions": {
                "INTERMEDIATE_VIEW": {
                    "query": {
                        "from": {"ref": ["RAD_S3X.SHARED_LOCAL_TABLE"]},
                    },
                    "elements": {
                        "KPI_VAL": {"type": "cds.Integer"}
                    }
                }
            }
        }
        def read_local_table_side_effect(space, name):
            if space == "RAD_S3X" and name == "SHARED_LOCAL_TABLE":
                return {
                    "definitions": {
                        "SHARED_LOCAL_TABLE": {
                            "elements": {
                                "COMPANY_CODE": {"key": True, "type": "cds.String"},
                                "KPI_VAL": {"type": "cds.Integer"},
                            }
                        }
                    }
                }
            return None
        self.mock_client.read_local_table.side_effect = read_local_table_side_effect

        resolver = DatasphereDependencyResolver(
            client=self.mock_client,
            space="RADH_S3X",
            replication_space="RAD_HDLF",
            silent=True,
        )

        tree = resolver.resolve("AM_TEST_MODEL", object_type="analytic_model")
        self.assertEqual(len(tree["dependencies"]), 1)
        inter = tree["dependencies"][0]
        self.assertEqual(inter["name"], "INTERMEDIATE_VIEW")
        self.assertEqual(len(inter["dependencies"]), 1)
        term = inter["dependencies"][0]
        self.assertEqual(term["name"], "SHARED_LOCAL_TABLE")
        self.assertEqual(term["space"], "RAD_S3X")
        self.assertEqual(term["object_type"], "local_table")
        self.assertTrue(term["is_shared"])
        self.assertTrue(term["is_boundary"])

    def test_boundary_that_is_a_view_rather_than_local_table(self):
        """
        Scenario 2: Boundary node is a View with no further upstream dependencies.
        Resolver must accurately classify it as a view without forcing it to be a local table.
        """
        self.mock_client.list_analytic_models.return_value = [{"name": "AM_VIEW_BOUNDARY"}]
        self.mock_client.list_views.return_value = [{"name": "BOUNDARY_VIEW"}]
        self.mock_client.read_analytic_model.return_value = {
            "definitions": {
                "AM_VIEW_BOUNDARY": {
                    "query": {
                        "from": {"ref": ["BOUNDARY_VIEW"]},
                    },
                    "elements": {
                        "AMOUNT": {"type": "cds.Decimal"}
                    }
                }
            }
        }
        self.mock_client.read_view.return_value = {
            "definitions": {
                "BOUNDARY_VIEW": {
                    "elements": {
                        "AMOUNT": {"type": "cds.Decimal"}
                    }
                }
            }
        }

        resolver = DatasphereDependencyResolver(
            client=self.mock_client,
            space="RADH_S3X",
            replication_space="RAD_HDLF",
            silent=True,
        )

        tree = resolver.resolve("AM_VIEW_BOUNDARY", object_type="analytic_model")
        self.assertEqual(len(tree["dependencies"]), 1)
        term = tree["dependencies"][0]
        self.assertEqual(term["name"], "BOUNDARY_VIEW")
        self.assertEqual(term["object_type"], "view")
        self.assertTrue(term["is_boundary"])

    def test_source_whose_cds_name_differs_from_datasphere_name(self):
        """
        Scenario 3: A local table LT_CUST_DATA maps to S/4HANA CDS entity I_Customer via lineage.
        CDS name is derived from lineage and verified via ADT.
        """
        req = ValidationRequest(
            view_name="AM_DIFF",
            space_id="RADH_S3X",
            human_approved=False,
        )
        with patch("validation_service.DatasphereResolverClient") as mock_client_cls:
            mock_client = Mock()
            mock_client.cli_available = True
            mock_client.refresh_replication_local_table_index.return_value = []
            mock_client_cls.return_value = mock_client

            with patch("validation_service.DatasphereDependencyResolver") as mock_resolver_cls:
                mock_resolver = Mock()
                mock_resolver.resolve.return_value = {
                    "name": "AM_DIFF",
                    "object_type": "analytic_model",
                    "status": "resolved",
                    "definition": {"dummy": True},
                    "field_catalog": [{"name": "CustomerNumber", "is_key": True}],
                    "dependencies": [
                        {
                            "name": "LT_CUST_DATA",
                            "object_type": "local_table",
                            "status": "resolved",
                            "definition": {
                                "sourceLineage": {"cdsEntity": "I_Customer"},
                            },
                            "is_boundary": True,
                            "field_catalog": [{"name": "CustomerNumber"}],
                            "dependencies": [],
                        }
                    ],
                }
                mock_resolver_cls.return_value = mock_resolver
                mock_client.cli_reads = {("analytic_model", "am_diff"), ("local_table", "lt_cust_data")}

                with patch("validation_service.CDSMetadataClient") as mock_adt_cls:
                    mock_adt = Mock()
                    mock_adt.discover_metadata.return_value = {
                        "entity": "I_Customer",
                        "fields": [{"name": "CustomerNumber"}],
                    }
                    mock_adt_cls.return_value.__enter__.return_value = mock_adt

                    resp = asyncio.run(run_metadata_driven_validation(req))
                    self.assertEqual(resp.source_cds_view, "I_Customer")
                    arch = resp.architecture_metadata
                    discovered = arch.get("s4hana_sources", [])
                    self.assertIn("I_Customer", discovered)

    def test_source_names_match_but_identity_unverified(self):
        """
        Scenario 4: Datasphere object is named ZI_SAMPLE. If ADT discovery fails
        or returns zero fields, the status must remain INDETERMINATE, never a false MATCH.
        """
        req = ValidationRequest(
            view_name="AM_UNVERIFIED",
            space_id="RADH_S3X",
            human_approved=False,
        )
        with patch("validation_service.DatasphereResolverClient") as mock_client_cls:
            mock_client = Mock()
            mock_client.cli_available = True
            mock_client.refresh_replication_local_table_index.return_value = []
            mock_client_cls.return_value = mock_client

            with patch("validation_service.DatasphereDependencyResolver") as mock_resolver_cls:
                mock_resolver = Mock()
                mock_resolver.resolve.return_value = {
                    "name": "AM_UNVERIFIED",
                    "object_type": "analytic_model",
                    "status": "resolved",
                    "definition": {"dummy": True},
                    "field_catalog": [{"name": "ColA", "is_key": True}],
                    "dependencies": [
                        {
                            "name": "ZI_SAMPLE",
                            "object_type": "local_table",
                            "status": "resolved",
                            "definition": {"dummy": True},
                            "is_boundary": True,
                            "field_catalog": [{"name": "ColA"}],
                            "dependencies": [],
                        }
                    ],
                }
                mock_resolver_cls.return_value = mock_resolver
                mock_client.cli_reads = {("analytic_model", "am_unverified"), ("local_table", "zi_sample")}

                with patch("validation_service.CDSMetadataClient") as mock_adt_cls:
                    mock_adt = Mock()
                    mock_adt.discover_metadata.return_value = {"entity": "ZI_SAMPLE", "fields": []}
                    mock_adt_cls.return_value.__enter__.return_value = mock_adt

                    resp = asyncio.run(run_metadata_driven_validation(req))
                    self.assertEqual(resp.status, "INDETERMINATE")
                    self.assertFalse(resp.comparison["is_match"])

    def test_calculated_columns_referencing_multiple_physical_fields(self):
        """
        Scenario 5: Model contains a calculated column derived from multiple physical fields.
        The compiler must substitute physical fields accurately into ABAP Open SQL without corrupting operators.
        """
        architecture = {
            "dependency_tree": {
                "field_catalog": [
                    {"name": "Net_Margin", "is_measure": True, "aggregation": "SUM"},
                    {"name": "Category", "is_measure": False},
                ],
                "calculated_columns": [
                    {
                        "name": "Net_Margin",
                        "expression": "(Revenue - Cost) * 100 / Revenue",
                    }
                ],
            }
        }
        source_metadata = [
            {
                "entity": "I_SalesAnalytics",
                "fields": [
                    {"name": "Revenue"},
                    {"name": "Cost"},
                    {"name": "Category"},
                ],
            }
        ]

        result = compile_reconciliation_blueprint_deterministic(
            model_name="AM_MARGIN",
            space_id="RADH_S3X",
            architecture_metadata=architecture,
            source_metadata=source_metadata,
            selected_fields=["Net_Margin"],
            key_fields=["Category"],
        )

        s4_sql = result["s4hana_sql"]
        self.assertIn("I_SalesAnalytics~Revenue", s4_sql)
        self.assertIn("I_SalesAnalytics~Cost", s4_sql)
        self.assertIn("SUM(", s4_sql)
        self.assertIn("GROUP BY\n    I_SalesAnalytics~Category", s4_sql)

    def test_filters_and_aggregations_across_intermediate_views(self):
        """
        Scenario 6: Complex CASE expression and measure aggregations across views.
        String literals inside CASE statements must not have their words substituted as column names.
        """
        architecture = {
            "dependency_tree": {
                "field_catalog": [
                    {"name": "KPI_Breach", "is_measure": False},
                    {"name": "Total_Amount", "source_field": "Amount", "is_measure": True, "aggregation": "SUM"},
                ],
                "calculated_columns": [
                    {
                        "name": "KPI_Breach",
                        "expression": "CASE WHEN Amount > 1000 THEN 'High Value Order' ELSE 'Standard' END",
                    }
                ],
            }
        }
        source_metadata = [
            {
                "entity": "ZI_ORDERS",
                "fields": [
                    {"name": "Amount"},
                    {"name": "Order"},
                ],
            }
        ]

        result = compile_reconciliation_blueprint_deterministic(
            model_name="AM_ORDERS",
            space_id="RADH_S3X",
            architecture_metadata=architecture,
            source_metadata=source_metadata,
            selected_fields=["Total_Amount"],
            key_fields=["KPI_Breach"],
        )

        s4_sql = result["s4hana_sql"]
        self.assertIn("'High Value Order'", s4_sql)
        self.assertNotIn("src~Order'", s4_sql)
        self.assertIn("ZI_ORDERS~Amount", s4_sql)
        self.assertIn("FROM ZI_ORDERS", s4_sql)
        self.assertNotIn(" AS src", s4_sql)

    def test_dynamic_fallback_preserves_filter_calculation_aggregation_and_entity_field_refs(self):
        architecture = {
            "dependency_tree": {
                "field_catalog": [
                    {"name": "Region", "source_field": "SalesRegion"},
                    {"name": "ConditionalTotal", "is_measure": True, "aggregation": "SUM"},
                ],
                "calculated_columns": [{
                    "name": "ConditionalTotal",
                    "expression": "CASE WHEN Status = 'A' THEN Amount ELSE 0 END",
                }],
            },
            "transformation_ir": {
                "projections": [{
                    "output_field": "Region",
                    "source_field": "SalesRegion",
                }],
                "filters": [{
                    "path": "FV_Sales.query.SELECT.where",
                    "condition": "Status = 'A'",
                }, {
                    "path": "FV_Sales.query.SELECT.having",
                    "condition": "SUM(Amount) > 10",
                }],
                "joins": [],
                "calculations": [{
                    "name": "ConditionalTotal",
                    "expression": "CASE WHEN Status = 'A' THEN Amount ELSE 0 END",
                }],
                "formulas": [],
                "aggregations": [{
                    "name": "ConditionalTotal",
                    "aggregation": "SUM",
                }],
                "groupings": [],
                "constants": [],
                "unsupported_constructs": [],
            },
        }
        source_metadata = [{
            "entity": "ZI_DynamicSales",
            "fields": [
                {"name": "SalesRegion"},
                {"name": "Status"},
                {"name": "Amount"},
            ],
        }]

        plan = compile_reconciliation_blueprint_deterministic(
            model_name="AM_DynamicSales",
            space_id="SPACE_X",
            architecture_metadata=architecture,
            source_metadata=source_metadata,
            selected_fields=["Region", "ConditionalTotal"],
            key_fields=[],
        )
        sql = plan["s4hana_sql"]
        self.assertIn("ZI_DynamicSales~SalesRegion AS Region", sql)
        self.assertIn(
            "SUM( CASE WHEN ZI_DynamicSales~Status = 'A' THEN ZI_DynamicSales~Amount ELSE 0 END ) AS ConditionalTotal",
            sql,
        )
        self.assertIn("FROM ZI_DynamicSales", sql)
        self.assertIn("WHERE ZI_DynamicSales~Status = 'A'", sql)
        self.assertIn("GROUP BY\n    ZI_DynamicSales~SalesRegion", sql)
        self.assertIn("HAVING SUM(ZI_DynamicSales~Amount) > 10", sql)
        self.assertNotIn(" AS src", sql)

    def test_conditional_aggregates_and_calculated_groups_are_projected_at_exact_grain(self):
        architecture = {
            "dependency_tree": {
                "field_catalog": [
                    {"name": "NetValue", "is_measure": True, "aggregation": "SUM"},
                    {"name": "RiskBand", "is_measure": False},
                    {"name": "PeriodYear", "is_measure": False},
                    {"name": "PeriodMonth", "is_measure": False},
                    {"name": "KPI_Title", "is_measure": False},
                ],
            },
            "transformation_ir": {
                "calculations": [
                    {
                        "name": "RiskBand",
                        "expression": "CASE WHEN Classification = 'R' THEN 'RISK' ELSE 'OTHER' END",
                    },
                    {"name": "PeriodYear", "expression": "LEFT(TxnDate, 4)"},
                    {"name": "PeriodMonth", "expression": "SUBSTRING(TxnDate, 5, 2)"},
                    {
                        "name": "NetValue",
                        "expression": "CASE WHEN RiskBand = 'RISK' THEN Amount ELSE 0 END",
                    },
                ],
                "aggregations": [{"name": "NetValue", "aggregation": "SUM"}],
                "constants": [{
                    "output_field": "KPI_Title",
                    "value": "Operations KPI",
                }],
                "groupings": [{
                    "path": "FV_Operations.query.SELECT.groupBy",
                    "expression": [
                        {"ref": ["RiskBand"]},
                        {"ref": ["PeriodYear"]},
                        {"ref": ["PeriodMonth"]},
                        {"ref": ["TxnDate"]},
                        {"ref": ["KPI_Title"]},
                    ],
                }],
                "filters": [],
                "joins": [],
                "formulas": [],
                "unsupported_constructs": [],
            },
        }
        sources = [{
            "entity": "ZI_Operations",
            "fields": [
                {"name": "Classification"},
                {"name": "Amount"},
                {"name": "TxnDate"},
            ],
        }]

        plan = compile_reconciliation_blueprint_deterministic(
            model_name="AM_Operations",
            space_id="SPACE_OPERATIONS",
            architecture_metadata=architecture,
            source_metadata=sources,
            selected_fields=["NetValue", "RiskBand", "KPI_Title"],
            key_fields=[],
        )

        sql = plan["s4hana_sql"]
        self.assertIn(
            "SUM( CASE WHEN (CASE WHEN ZI_Operations~Classification = 'R' "
            "THEN 'RISK' ELSE 'OTHER' END) = 'RISK' "
            "THEN ZI_Operations~Amount ELSE 0 END ) AS NetValue",
            sql,
        )
        self.assertIn(
            "CASE WHEN ZI_Operations~Classification = 'R' "
            "THEN 'RISK' ELSE 'OTHER' END AS RiskBand",
            sql,
        )
        self.assertIn(
            "substring( val = ZI_Operations~TxnDate off = 0 len = 4 ) AS DS_GROUP_2",
            sql,
        )
        self.assertIn(
            "substring( val = ZI_Operations~TxnDate off = 4 len = 2 ) AS DS_GROUP_3",
            sql,
        )
        self.assertIn("ZI_Operations~TxnDate AS DS_GROUP_4", sql)
        self.assertIn("'Operations KPI' AS KPI_Title", sql)
        self.assertIn(
            "GROUP BY\n    CASE WHEN ZI_Operations~Classification = 'R' "
            "THEN 'RISK' ELSE 'OTHER' END,\n"
            "    substring( val = ZI_Operations~TxnDate off = 0 len = 4 ),\n"
            "    substring( val = ZI_Operations~TxnDate off = 4 len = 2 ),\n"
            "    ZI_Operations~TxnDate,\n"
            "    'Operations KPI'",
            sql,
        )
        _validate_reconciliation_sql(
            _project_open_sql_grouping_expressions(sql),
            expected_sources=["ZI_Operations"],
            allowed_source_fields=["Classification", "Amount", "TxnDate"],
            source_field_map={
                "ZI_Operations": ["Classification", "Amount", "TxnDate"],
            },
            enforce_abap_line_limit=True,
            enforce_entity_qualified_fields=True,
        )

    def test_deterministic_fallback_refuses_uncompiled_joins_and_missing_field_mappings(self):
        source_metadata = [{
            "entity": "ZI_OnlySource",
            "fields": [{"name": "KnownField"}],
        }]
        with self.assertRaisesRegex(ValueError, "has an unresolved source"):
            compile_reconciliation_blueprint_deterministic(
                model_name="AM_X",
                space_id="SPACE_X",
                architecture_metadata={
                    "dependency_tree": {"field_catalog": []},
                    "transformation_ir": {"joins": [{"condition": "A.ID = B.ID"}]},
                },
                source_metadata=source_metadata,
                selected_fields=["KnownField"],
                key_fields=[],
            )
        with self.assertRaisesRegex(ValueError, "No verified source field"):
            compile_reconciliation_blueprint_deterministic(
                model_name="AM_X",
                space_id="SPACE_X",
                architecture_metadata={"dependency_tree": {"field_catalog": []}},
                source_metadata=source_metadata,
                selected_fields=["InventedField"],
                key_fields=[],
            )

    def test_nested_calculated_filters_compile_for_another_discovered_model(self):
        architecture = {
            "dependency_tree": {
                "field_catalog": [
                    {"name": "MarketRegion", "source_field": "SalesArea"},
                    {"name": "DemandValue", "is_measure": True, "aggregation": "SUM"},
                ],
            },
            "transformation_ir": {
                "projections": [{
                    "output_field": "MarketRegion",
                    "source_field": "SalesArea",
                }],
                "calculations": [
                    {"name": "ShortCode", "expression": "LEFT(ProductCode, 2)"},
                    {
                        "name": "ScaledQuantity",
                        "expression": "Quantity * 2",
                    },
                    {"name": "QualifiedDemand", "expression": "ScaledQuantity * UnitPrice"},
                    {"name": "DemandValue", "expression": "QualifiedDemand"},
                    {
                        "name": "DemandClass",
                        "expression": "CASE WHEN ShortCode = 'AB' THEN 'NORTH' ELSE 'SOUTH' END",
                    },
                ],
                "filters": [{
                    "path": "SalesDemandView.query.SELECT.where",
                    "condition": "QualifiedDemand > 0",
                }],
                "aggregations": [{"name": "DemandValue", "aggregation": "SUM"}],
                "joins": [],
                "formulas": [],
                "groupings": [],
                "constants": [],
                "unsupported_constructs": [],
            },
        }
        source_metadata = [{
            "entity": "ZI_SalesDemand",
            "fields": [
                {"name": "SalesArea"},
                {"name": "ProductCode"},
                {"name": "Quantity"},
                {"name": "UnitPrice"},
            ],
        }]

        plan = compile_reconciliation_blueprint_deterministic(
            model_name="AM_SalesDemand",
            space_id="SPACE_RETAIL",
            architecture_metadata=architecture,
            source_metadata=source_metadata,
            selected_fields=["DemandValue"],
            key_fields=["MarketRegion", "DemandClass"],
        )

        sql = plan["s4hana_sql"]
        self.assertIn("ZI_SalesDemand~SalesArea AS MarketRegion", sql)
        self.assertIn("CASE WHEN", sql)
        self.assertIn("substring( val = ZI_SalesDemand~ProductCode off = 0 len = 2 )", sql)
        self.assertIn(
            "WHERE ((ZI_SalesDemand~Quantity * 2) * ZI_SalesDemand~UnitPrice) > 0",
            sql,
        )
        self.assertNotIn("QualifiedDemand", sql)
        self.assertNotIn("ShortCode", sql)
        coverage = _verify_transformation_coverage(
            architecture["transformation_ir"],
            s4hana_sql=sql,
            selected_fields=["MarketRegion", "DemandClass", "DemandValue"],
            target_columns=architecture["dependency_tree"]["field_catalog"],
            source_fields=[
                {"name": field["name"]}
                for field in source_metadata[0]["fields"]
            ],
        )
        self.assertEqual(coverage["overall_status"], "VERIFIED")

    def test_dynamic_join_filter_and_measure_compile_from_discovered_sources(self):
        architecture = {
            "dependency_tree": {
                "field_catalog": [
                    {"name": "Territory", "source_field": "TerritoryName"},
                    {"name": "TotalSales", "is_measure": True, "aggregation": "SUM"},
                ],
            },
            "transformation_ir": {
                "projections": [{
                    "output_field": "Territory",
                    "source_field": "TerritoryName",
                }],
                "calculations": [
                    {"name": "OrderValue", "expression": "Units * UnitPrice"},
                    {"name": "TotalSales", "expression": "OrderValue"},
                    {
                        "name": "PremiumOrder",
                        "expression": "CASE WHEN UnitPrice >= 100 THEN 'Y' ELSE 'N' END",
                    },
                ],
                "filters": [{
                    "path": "OrderFacts.query.SELECT.where",
                    "condition": "PremiumOrder = 'Y'",
                }],
                "aggregations": [{"name": "TotalSales", "aggregation": "SUM"}],
                "joins": [{
                    "path": "OrderFacts.query.from.join",
                    "join_type": "left outer",
                    "left": {"source": "ZI_OrderItem", "as": "Orders"},
                    "right": {"source": "ZI_Territory", "as": "Areas"},
                    "on": "Orders.TerritoryID = Areas.TerritoryID",
                }],
                "formulas": [],
                "groupings": [],
                "constants": [],
                "unsupported_constructs": [],
            },
        }
        source_metadata = [
            {
                "entity": "ZI_OrderItem",
                "fields": [
                    {"name": "TerritoryID"},
                    {"name": "Units"},
                    {"name": "UnitPrice"},
                ],
            },
            {
                "entity": "ZI_Territory",
                "fields": [
                    {"name": "TerritoryID"},
                    {"name": "TerritoryName"},
                ],
            },
        ]

        plan = compile_reconciliation_blueprint_deterministic(
            model_name="AM_OrderSales",
            space_id="SPACE_ORDERS",
            architecture_metadata=architecture,
            source_metadata=source_metadata,
            selected_fields=["TotalSales"],
            key_fields=["Territory"],
        )

        sql = plan["s4hana_sql"]
        self.assertIn(
            "LEFT OUTER JOIN ZI_Territory ON "
            "ZI_OrderItem~TerritoryID = ZI_Territory~TerritoryID",
            sql,
        )
        self.assertIn(
            "SUM( (ZI_OrderItem~Units * ZI_OrderItem~UnitPrice) ) AS TotalSales",
            sql,
        )
        self.assertIn("WHERE ZI_OrderItem~UnitPrice >= 100", sql)
        self.assertIn("GROUP BY\n    ZI_Territory~TerritoryName", sql)
        self.assertNotIn(" AS Orders", sql)
        coverage = _verify_transformation_coverage(
            architecture["transformation_ir"],
            s4hana_sql=sql,
            selected_fields=["Territory", "TotalSales"],
            target_columns=architecture["dependency_tree"]["field_catalog"],
            source_fields=[
                {"name": field["name"]}
                for source in source_metadata
                for field in source["fields"]
            ],
        )
        self.assertEqual(coverage["overall_status"], "VERIFIED")

    def test_case_filter_is_not_reduced_when_else_branch_can_match(self):
        architecture = {
            "dependency_tree": {
                "field_catalog": [{"name": "Amount"}],
                "calculated_columns": [{
                    "name": "AlwaysYes",
                    "expression": "CASE WHEN Amount > 0 THEN 'Y' ELSE 'Y' END",
                }],
            },
            "transformation_ir": {
                "filters": [{
                    "path": "Orders.query.SELECT.where",
                    "condition": "AlwaysYes = 'Y'",
                }],
                "joins": [],
                "calculations": [{
                    "name": "AlwaysYes",
                    "expression": "CASE WHEN Amount > 0 THEN 'Y' ELSE 'Y' END",
                }],
                "formulas": [],
                "aggregations": [],
                "groupings": [],
                "constants": [],
                "unsupported_constructs": [],
            },
        }

        plan = compile_reconciliation_blueprint_deterministic(
            model_name="AM_CaseSemantics",
            space_id="SPACE_CASES",
            architecture_metadata=architecture,
            source_metadata=[{
                "entity": "ZI_CaseAmounts",
                "fields": [{"name": "Amount"}],
            }],
            selected_fields=["Amount"],
            key_fields=[],
        )

        self.assertIn(
            "WHERE (CASE WHEN ZI_CaseAmounts~Amount > 0 THEN 'Y' ELSE 'Y' END) = 'Y'",
            plan["s4hana_sql"],
        )

    def test_datasphere_group_by_reference_objects_compile_as_individual_fields(self):
        architecture = {
            "dependency_tree": {
                "field_catalog": [
                    {"name": "Posting_Year", "is_calculated": True},
                    {"name": "Posting_Month", "is_calculated": True},
                    {"name": "PostingDate"},
                    {"name": "KPI_Breach", "is_calculated": True},
                    {"name": "KPI__Name", "is_calculated": True},
                ],
            },
            "transformation_ir": {
                "projections": [],
                "filters": [],
                "joins": [],
                "calculations": [
                    {
                        "name": "Posting_Year",
                        "expression": "LEFT(PostingDate, 4)",
                        "transformation_category": "function",
                    },
                    {
                        "name": "Posting_Month",
                        "expression": "SUBSTRING(PostingDate, 5, 2)",
                        "transformation_category": "function",
                    },
                    {
                        "name": "KPI_Breach",
                        "expression": "CASE WHEN DaysOverdue > 40 THEN 'X' ELSE ' ' END",
                    },
                    {
                        "name": "KPI__Name",
                        "expression": "'Customer Receivables KPI'",
                        "transformation_category": "constant",
                    },
                ],
                "formulas": [],
                "aggregations": [],
                "groupings": [{
                    "path": "FV_Receivables.query.SELECT.groupBy",
                    "expression": [
                        {"ref": ["Posting_Year"]},
                        {"ref": ["Posting_Month"]},
                        {"ref": ["PostingDate"]},
                        {"ref": ["KPI_Breach"]},
                        {"ref": ["KPI__Name"]},
                    ],
                }],
                "constants": [],
                "unsupported_constructs": [],
            },
        }

        plan = compile_reconciliation_blueprint_deterministic(
            model_name="AM_Receivables",
            space_id="SPACE_FINANCE",
            architecture_metadata=architecture,
            source_metadata=[{
                "entity": "ZI_Receivables",
                "fields": [{"name": "PostingDate"}, {"name": "DaysOverdue"}],
            }],
            selected_fields=["KPI_Breach"],
            key_fields=["Posting_Year", "Posting_Month", "PostingDate", "KPI__Name"],
        )

        sql = plan["s4hana_sql"]
        self.assertIn(
            "substring( val = ZI_Receivables~PostingDate off = 0 len = 4 )",
            sql,
        )
        self.assertIn(
            "substring( val = ZI_Receivables~PostingDate off = 4 len = 2 )",
            sql,
        )
        self.assertIn("ZI_Receivables~PostingDate", sql)
        self.assertIn(
            "CASE WHEN ZI_Receivables~DaysOverdue > 40 THEN 'X' ELSE ' ' END",
            sql,
        )
        self.assertIn("'Customer Receivables KPI'", sql)
        self.assertNotIn("{'ref'", sql)

    def test_s4hana_source_format_requires_entity_qualified_fields_without_aliases(self):
        _validate_reconciliation_sql(
            "SELECT ZI_DynamicSales~SalesRegion AS Region FROM ZI_DynamicSales",
            expected_sources=["ZI_DynamicSales"],
            allowed_source_fields=["SalesRegion"],
            enforce_entity_qualified_fields=True,
        )
        with self.assertRaisesRegex(ValueError, "source aliases"):
            _validate_reconciliation_sql(
                "SELECT src~SalesRegion AS Region FROM ZI_DynamicSales AS src",
                expected_sources=["ZI_DynamicSales"],
                allowed_source_fields=["SalesRegion"],
                enforce_entity_qualified_fields=True,
            )
        with self.assertRaisesRegex(ValueError, "must be qualified"):
            _validate_reconciliation_sql(
                "SELECT SalesRegion AS Region FROM ZI_DynamicSales",
                expected_sources=["ZI_DynamicSales"],
                allowed_source_fields=["SalesRegion"],
                enforce_entity_qualified_fields=True,
            )
        with self.assertRaisesRegex(ValueError, "not present on verified CDS entity"):
            _validate_reconciliation_sql(
                "SELECT ZI_DynamicSales~Amount FROM ZI_DynamicSales",
                expected_sources=["ZI_DynamicSales"],
                allowed_source_fields=["Amount", "SalesRegion"],
                source_field_map={"ZI_DynamicSales": ["SalesRegion"]},
                enforce_entity_qualified_fields=True,
            )

    def test_unsupported_sql_translations_rejected(self):
        """
        Scenario 8: Forbidden statements (semicolons, comments, DML) must be rejected.
        """
        with self.assertRaises(ValueError):
            _validate_reconciliation_sql(
                "SELECT S~Amount FROM ZI_ORDERS AS S; DROP TABLE ZI_ORDERS",
                expected_sources=["ZI_ORDERS"],
                allowed_source_fields=["Amount"],
            )

        with self.assertRaises(ValueError):
            _validate_reconciliation_sql(
                "SELECT S~Amount FROM ZI_ORDERS AS S -- dangerous comment",
                expected_sources=["ZI_ORDERS"],
                allowed_source_fields=["Amount"],
            )

    def test_derive_model_keys_for_aggregated_and_leaf_models(self):
        """
        Scenario: Output grain derived dynamically from dimensions for aggregated models
        and leaf keys for leaf models without hardcoded assumptions.
        """
        # Aggregated model
        agg_tree = {
            "field_catalog": [
                {"name": "FiscalYear", "is_measure": False, "is_key": False},
                {"name": "CompanyCode", "is_measure": False, "is_key": False},
                {"name": "Revenue", "is_measure": True, "measure_type": "AMOUNT"},
            ]
        }
        keys = _derive_model_keys(agg_tree, replicated_sources=[])
        self.assertEqual(keys, ["FiscalYear", "CompanyCode"])

        # Leaf model with explicit key
        leaf_tree = {
            "field_catalog": [
                {"name": "DocumentID", "is_measure": False, "is_key": True},
                {"name": "ItemNo", "is_measure": False, "is_key": True},
                {"name": "Description", "is_measure": False, "is_key": False},
            ]
        }
        keys_leaf = _derive_model_keys(leaf_tree, replicated_sources=[])
        self.assertEqual(keys_leaf, ["DocumentID", "ItemNo"])

    def test_unapproved_plan_requires_approval_and_never_fabricates_match(self):
        """
        Scenario 6 & 8: Unapproved execution returns status NOT_RUN with approval_required=True
        and never produces a false MATCH.
        """
        req = ValidationRequest(
            view_name="AM_TEST",
            space_id="RADH_S3X",
            human_approved=False,
        )
        with patch("validation_service.DatasphereResolverClient") as mock_client_cls:
            mock_client = Mock()
            mock_client.cli_available = True
            mock_client.refresh_replication_local_table_index.return_value = []
            mock_client_cls.return_value = mock_client

            with patch("validation_service.DatasphereDependencyResolver") as mock_resolver_cls:
                mock_resolver = Mock()
                mock_resolver.resolve.return_value = {
                    "name": "AM_TEST",
                    "object_type": "analytic_model",
                    "status": "resolved",
                    "definition": {"dummy": True},
                    "field_catalog": [{"name": "Amount", "is_measure": True}, {"name": "Year", "is_measure": False}],
                    "dependencies": [
                        {
                            "name": "FV_TEST",
                            "object_type": "view",
                            "relationship": "analytic_model_source",
                            "status": "resolved",
                            "runtime_object": "FV_TEST_RT",
                            "schema": "RADH_S3X",
                            "definition": {"definitions": {"FV_TEST": {}}},
                            "field_catalog": [
                                {"name": "Amount", "aggregation": "SUM"},
                                {"name": "Year", "is_key": True},
                            ],
                            "dependencies": [{
                                "name": "ZI_SRC",
                                "object_type": "local_table",
                                "status": "resolved",
                                "definition": {"sourceLineage": {"cdsEntity": "ZI_SRC"}},
                                "is_boundary": True,
                                "field_catalog": [{"name": "Amount"}, {"name": "Year"}],
                                "dependencies": [],
                            }],
                        }
                    ],
                }
                mock_resolver_cls.return_value = mock_resolver
                mock_client.cli_reads = {("analytic_model", "am_test"), ("local_table", "zi_src")}

                with patch("validation_service.CDSMetadataClient") as mock_adt_cls:
                    mock_adt = Mock()
                    mock_adt.discover_metadata.return_value = {
                        "entity": "ZI_SRC",
                        "fields": [{"name": "Amount"}, {"name": "Year"}],
                        "key_fields": ["Year"],
                        "parameters": [],
                    }
                    mock_adt_cls.return_value.__enter__.return_value = mock_adt

                    resp = asyncio.run(run_metadata_driven_validation(req))

                    self.assertEqual(resp.status, "NOT_RUN")
                    self.assertTrue(resp.approval_required)
                    self.assertFalse(resp.comparison["is_match"])
                    self.assertIsNotNone(resp.execution_plan)
                    self.assertTrue(resp.execution_plan.get("query_hash"))


def test_llm_reconstruction_prompt_contains_discovered_transformations_and_sources():
    prompt_values = []
    generated_sql = (
        "SELECT CASE WHEN ZI_CUSTOMERRECEIVABLESKPI~DaysOverdue > 40 THEN 'X' ELSE ' ' END AS KPI_Breach "
        "FROM ZI_CUSTOMERRECEIVABLESKPI WHERE ZI_CUSTOMERRECEIVABLESKPI~DaysOverdue > 40"
    )

    async def mock_claude(prompt, key):
        prompt_values.append(prompt)
        return {
            "datasphere_sql": 'SELECT "KPI_Breach" FROM "RADH_S3X"."AM_Customer_Receivables_KPI"',
            "s4hana_sql": generated_sql,
            "output_fields": ["KPI_Breach"],
            "key_fields": [],
            "field_mappings": [],
            "filters": [],
            "joins": [],
            "calculated_expressions": [],
            "aggregation_semantics": [],
            "parameter_bindings": [],
            "constants": [],
            "groupings": [],
            "null_handling": [],
            "unsupported_transformations": [],
            "logic_summary": "Reconstruct receivables KPI breach logic.",
            "assumptions": [],
        }

    async def no_examples(*args, **kwargs):
        return []

    architecture = {
        "transformation_ir": {
            "calculations": [{
                "owner": "Customer_Receivable_G_View",
                "name": "KPI_Breach",
                "expression": "CASE WHEN ZI_CUSTOMERRECEIVABLESKPI.DaysOverdue > 40 THEN 'X' ELSE ' ' END",
                "path": "Customer_Receivable_G_View.KPI_Breach",
            }],
            "filters": [{
                "owner": "FV_Customer_Receivables",
                "condition": "Customer_Receivable_G_View.KPI_Breach = 'X'",
                "path": "FV_Customer_Receivables.query.SELECT.where",
            }],
        },
        "dependency_tree": {
            "name": "AM_Customer_Receivables_KPI",
            "definition": {"businessLayerDefinitions": {"factSources": ["FV_Customer_Receivables"]}},
        },
    }
    source_metadata = [{
        "entity": "ZI_CUSTOMERRECEIVABLESKPI",
        "fields": [{"name": "DaysOverdue", "type": "INT4"}],
        "source_ddl": "define view ZI_CUSTOMERRECEIVABLESKPI ...",
    }]

    with patch.object(llm_client, "ANTHROPIC_API_KEY", "mock-key"), \
         patch.object(llm_client, "call_claude_api", mock_claude), \
         patch.object(llm_client.rag_service, "retrieve_golden_sql_examples", no_examples), \
         patch.object(llm_client.rag_service, "retrieve_glossary_terms", no_examples):
        result = asyncio.run(llm_client.generate_reconciliation_query_blueprint(
            user_prompt="show KPI breach",
            model_name="AM_Customer_Receivables_KPI",
            space_id="RADH_S3X",
            architecture_metadata=architecture,
            source_metadata=source_metadata,
            selected_fields=["KPI_Breach"],
            key_fields=[],
        ))

    assert prompt_values
    assert "KPI_Breach = 'X'" in prompt_values[0]
    assert "DaysOverdue > 40" in prompt_values[0]
    assert "ZI_CUSTOMERRECEIVABLESKPI" in prompt_values[0]
    assert "Do not assign source aliases" in prompt_values[0]
    assert result["s4hana_sql"] == generated_sql


if __name__ == "__main__":
    unittest.main()
