"""Tests for the lineage-platform bindings added in conduit-native 0.2.0.

Covers PRD Epic D stories D1-D3: dbt-manifest-aware extraction, contract
validation, plan-impact analysis, and OpenLineage emit. Run with the module
built into the active environment (maturin develop).
"""

import json
import pathlib

import pytest
from conduit_native import compiler, lineage

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
IMPACT_FIXTURES = REPO_ROOT / "conduit-cli" / "tests" / "fixtures" / "impact"

MANIFEST = json.dumps(
    {
        "nodes": {
            "model.demo.users": {
                "name": "users",
                "resource_type": "model",
                "database": "analytics",
                "schema": "marts",
                "alias": "dim_users",
                "package_name": "demo",
            }
        },
        "sources": {},
    }
)


# ─── D1: extract_sql_lineage_full (dbt manifest) ─────────────────────────────


class TestExtractFull:
    SQL = "SELECT id, email FROM {{ ref('users') }}"

    def test_manifest_resolves_ref_to_real_table(self):
        result = json.loads(
            lineage.extract_sql_lineage_full(self.SQL, dbt_manifest=MANIFEST)
        )
        assert "dim_users" in result["input_tables"]
        assert any(
            t.endswith(".dim_users") for t in result["input_tables_qualified"]
        ), f"expected a qualified ref: {result['input_tables_qualified']}"

    def test_without_manifest_ref_falls_back_to_placeholder(self):
        result = json.loads(lineage.extract_sql_lineage_full(self.SQL))
        assert not any(
            "dim_users" in table for table in result["input_tables"]
        ), "ref() must NOT resolve without a manifest"

    def test_manifest_from_file(self, tmp_path):
        manifest_path = tmp_path / "manifest.json"
        manifest_path.write_text(MANIFEST)
        result = json.loads(
            lineage.extract_sql_lineage_full(
                self.SQL, dbt_manifest=str(manifest_path)
            )
        )
        assert "dim_users" in result["input_tables"]

    def test_invalid_manifest_json_raises(self):
        with pytest.raises(ValueError):
            lineage.extract_sql_lineage_full(self.SQL, dbt_manifest="{not json")

    def test_catalog_and_dialect_still_apply(self):
        catalog = json.dumps({"orders": ["id", "amount"]})
        result = json.loads(
            lineage.extract_sql_lineage_full(
                "SELECT amount FROM orders", catalog_json=catalog, dialect="bigquery"
            )
        )
        deps = {d["output"]: d["sources"] for d in result["column_dependencies"]}
        assert deps["amount"] == ["orders.amount"]


# ─── D2: validate_contract ───────────────────────────────────────────────────


def make_schema(columns):
    return json.dumps(
        {
            "task_id": "build_orders",
            "dag_id": None,
            "columns": columns,
            "version": 1,
        }
    )


class TestValidateContract:
    def test_missing_required_column_fails(self):
        schema = make_schema(
            [{"name": "id", "column_type": "Integer", "nullable": False,
              "description": None, "tags": []}]
        )
        contract = json.dumps(
            {
                "task_id": "build_orders",
                "dag_id": None,
                "description": None,
                "rules": [
                    {
                        "RequiredColumn": {
                            "name": "order_id",
                            "expected_type": None,
                            "must_be_not_null": False,
                        }
                    }
                ],
            }
        )
        result = json.loads(lineage.validate_contract(schema, contract))
        assert result["passed"] is False
        assert result["violations"], "expected a violation for the missing column"

    def test_satisfied_contract_passes(self):
        schema = make_schema(
            [{"name": "order_id", "column_type": "Integer", "nullable": False,
              "description": None, "tags": []}]
        )
        contract = json.dumps(
            {
                "task_id": "build_orders",
                "dag_id": None,
                "description": None,
                "rules": [
                    {
                        "RequiredColumn": {
                            "name": "order_id",
                            "expected_type": None,
                            "must_be_not_null": False,
                        }
                    }
                ],
            }
        )
        result = json.loads(lineage.validate_contract(schema, contract))
        assert result["passed"] is True
        assert result["rules_checked"] == 1


# ─── D2: analyze_plan_impact ─────────────────────────────────────────────────


class TestPlanImpact:
    @pytest.fixture(scope="class")
    def plans(self):
        base = compiler.compile_dags_full(str(IMPACT_FIXTURES / "base"))
        head = compiler.compile_dags_full(str(IMPACT_FIXTURES / "head"))
        return base, head

    def test_dropped_column_is_breaking(self, plans):
        base, head = plans
        report = json.loads(lineage.analyze_plan_impact(base, head))
        assert report["summary"]["total_breaking_changes"] >= 1

    def test_identical_plans_are_clean(self, plans):
        base, _ = plans
        report = json.loads(lineage.analyze_plan_impact(base, base))
        assert report["summary"]["total_breaking_changes"] == 0

    def test_markdown_format_names_the_column(self, plans):
        base, head = plans
        report = lineage.analyze_plan_impact(base, head, format="markdown")
        assert "region" in report

    def test_invalid_format_raises(self, plans):
        base, _ = plans
        with pytest.raises(ValueError):
            lineage.analyze_plan_impact(base, base, format="pdf")


# ─── D3: to_openlineage_event ────────────────────────────────────────────────


class TestOpenLineageEmit:
    SQL = "INSERT INTO analytics.daily SELECT id, amount FROM staging.orders"

    def test_event_shape_and_column_lineage_facet(self):
        event = json.loads(
            lineage.to_openlineage_event(
                self.SQL,
                job_namespace="conduit",
                job_name="demo.transform",
                dataset_namespace="warehouse",
                output_dataset="analytics.daily",
            )
        )
        assert event["eventType"] == "COMPLETE"
        assert event["job"]["namespace"] == "conduit"
        outputs = event["outputs"]
        assert outputs and outputs[0]["name"] == "analytics.daily"
        facets = outputs[0].get("facets", {})
        assert "columnLineage" in facets, f"missing columnLineage facet: {facets}"

    def test_explicit_run_id_and_event_type(self):
        run_id = "00000000-0000-4000-8000-000000000000"
        event = json.loads(
            lineage.to_openlineage_event(
                self.SQL,
                job_namespace="conduit",
                job_name="demo.transform",
                dataset_namespace="warehouse",
                output_dataset="analytics.daily",
                event_type="START",
                run_id=run_id,
            )
        )
        assert event["eventType"] == "START"
        assert event["run"]["runId"] == run_id

    def test_invalid_event_type_raises(self):
        with pytest.raises(ValueError):
            lineage.to_openlineage_event(
                self.SQL,
                job_namespace="c",
                job_name="j",
                dataset_namespace="d",
                output_dataset="o",
                event_type="BOGUS",
            )

    def test_invalid_run_id_raises(self):
        with pytest.raises(ValueError):
            lineage.to_openlineage_event(
                self.SQL,
                job_namespace="c",
                job_name="j",
                dataset_namespace="d",
                output_dataset="o",
                run_id="not-a-uuid",
            )
