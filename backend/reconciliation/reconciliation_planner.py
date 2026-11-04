"""Conservative, metadata-driven reconciliation plan builder.

The planner does not generate executable SQL from free-form LLM output.
It accepts a normalized model specification and source metadata, then
produces a reviewable plan. Actual SQL rendering is a separate step.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any


class PlanningError(ValueError):
    pass


ALLOWED_AGGREGATIONS = {"SUM", "COUNT", "MIN", "MAX", "AVG", "COUNT_DISTINCT"}


@dataclass
class ReconciliationPlanner:
    registry: Any

    def build_plan(
        self,
        *,
        model_id: str,
        model_spec: dict[str, Any],
        dependencies: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """Build a plan from a normalized model_spec.

        Expected normalized shape:
        {
          "dimensions": ["CompanyCode", "FiscalYear"],
          "measures": [{
             "name": "GrossRevenue",
             "aggregation": "SUM",
             "source": "I_BILLINGDOCUMENTITEM",
             "source_field": "NetAmount",
             "model_field": "GrossRevenue",
             "dimensions": ["CompanyCode", "FiscalYear"],
             "filters": []
          }]
        }

        This is an input contract; it is not an automatic parser for every
        Datasphere analytical-model definition.
        """
        if not model_id:
            raise PlanningError("model_id is required")
        if not isinstance(model_spec, dict):
            raise PlanningError("model_spec must be a dictionary")

        deps = dependencies
        if deps is None:
            deps = self.registry.get_dependencies(model_id)

        dep_by_name: dict[str, list[dict[str, Any]]] = {}
        for dep in deps:
            name = str(dep.get("name") or dep.get("object_name") or "").strip().upper()
            if name:
                dep_by_name.setdefault(name, []).append(dep)

        measures = model_spec.get("measures") or []
        if not measures:
            raise PlanningError("No measures were supplied in model_spec.")

        checks = []
        warnings = []
        for measure in measures:
            name = str(measure.get("name") or "").strip()
            source_name = str(measure.get("source") or "").strip()
            source_field = str(measure.get("source_field") or "").strip()
            aggregation = str(measure.get("aggregation") or "").upper()
            dimensions = measure.get("dimensions", model_spec.get("dimensions", []))

            if not name or not source_name or not source_field:
                raise PlanningError(
                    "Each measure requires name, source, and source_field. "
                    f"Invalid measure: {measure!r}"
                )
            if aggregation not in ALLOWED_AGGREGATIONS:
                raise PlanningError(
                    f"Unsupported aggregation {aggregation!r} for measure {name!r}."
                )

            norm_source = source_name.upper()
            matching_dep = dep_by_name.get(norm_source, [])
            cached_matches = self.registry.find_by_name(source_name)

            if not matching_dep:
                # Accept a cached source only if lineage is explicitly marked
                # by the caller or resolver output. Name matching alone is not proof.
                explicitly_lineaged = [
                    d for d in deps
                    if str(d.get("source_id") or "").upper() in {
                        s["source_id"].upper() for s in cached_matches
                    }
                ]
                if not explicitly_lineaged:
                    raise PlanningError(
                        f"Source {source_name!r} is not confirmed in the supplied "
                        f"dependency graph for model {model_id!r}."
                    )

            source_record = cached_matches[0] if cached_matches else None
            if len(cached_matches) > 1:
                warnings.append(
                    f"Multiple cached sources named {source_name}; source-system "
                    "identity must be resolved before execution."
                )
            metadata = (source_record or {}).get("metadata", {})
            fields = metadata.get("fields", [])
            field_names = {
                str(f.get("name") if isinstance(f, dict) else f).upper()
                for f in fields
            }
            if fields and source_field.upper() not in field_names:
                raise PlanningError(
                    f"Field {source_field!r} was not found in cached metadata "
                    f"for source {source_name!r}."
                )

            if aggregation == "COUNT_DISTINCT":
                aggregation = "COUNT_DISTINCT"

            checks.append({
                "check_id": f"{model_id}:{name}",
                "measure": name,
                "model_field": measure.get("model_field") or name,
                "source": source_name,
                "source_id": source_record.get("source_id") if source_record else None,
                "source_field": source_field,
                "aggregation": aggregation,
                "dimensions": list(dimensions or []),
                "filters": list(measure.get("filters") or []),
                "parameters": dict(measure.get("parameters") or {}),
                "currency_field": measure.get("currency_field"),
                "unit_field": measure.get("unit_field"),
                "grain": measure.get("grain"),
                "requires_grain_review": not bool(measure.get("grain")),
                "status": "PLANNED",
            })

        if not deps:
            warnings.append("No dependency graph was supplied or cached.")

        return {
            "model_id": model_id,
            "plan_version": 1,
            "status": "REQUIRES_VALIDATION" if warnings else "PLANNED",
            "checks": checks,
            "warnings": warnings,
            "security": {
                "raw_business_data_sent_to_llm": False,
                "sql_requires_validation_before_execution": True,
            },
        }
