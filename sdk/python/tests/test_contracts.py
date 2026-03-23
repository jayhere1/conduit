"""Tests for the Conduit evidence-based contracts module."""
import json
import pytest
from io import StringIO

from conduit_sdk.contracts import (
    check,
    contract,
    Check,
    Contracts,
    emit_metric,
    emit_evidence,
    emit_row_count,
    emit_freshness_seconds,
    emit_freshness,
    emit_duplicate_count,
    emit_null_rate,
    emit_custom,
    assert_row_count,
    assert_freshness,
    assert_unique,
)


class TestCheckFactory:
    """Tests for the check builder factory."""

    def test_row_count_min(self):
        c = check.row_count(min=1)
        assert c.type == "row_count"
        assert c.params["min"] == 1

    def test_row_count_range(self):
        c = check.row_count(min=100, max=1_000_000)
        d = c.to_dict()
        assert d["type"] == "row_count"
        assert d["min"] == 100
        assert d["max"] == 1_000_000

    def test_row_count_exact(self):
        c = check.row_count(exact=42)
        assert c.params["exact"] == 42

    def test_freshness(self):
        c = check.freshness("24h")
        d = c.to_dict()
        assert d["max_age"] == "24h"

    def test_unique_string(self):
        c = check.unique("id")
        assert c.params["columns"] == ["id"]

    def test_unique_list(self):
        c = check.unique(["id", "date"])
        assert c.params["columns"] == ["id", "date"]

    def test_not_null_default(self):
        c = check.not_null("email")
        assert c.params["min_rate"] == 1.0

    def test_not_null_custom(self):
        c = check.not_null("phone", min_rate=0.8)
        assert c.params["min_rate"] == 0.8

    def test_accepted_values(self):
        c = check.accepted_values("status", ["active", "inactive"])
        d = c.to_dict()
        assert d["values"] == ["active", "inactive"]
        assert d["allow_null"] is False

    def test_value_range(self):
        c = check.value_range("amount", min=0, max=1_000_000)
        d = c.to_dict()
        assert d["min"] == 0
        assert d["max"] == 1_000_000

    def test_references(self):
        c = check.references("customer_id", "dim_customers", "id")
        d = c.to_dict()
        assert d["ref_task"] == "dim_customers"

    def test_row_count_delta(self):
        c = check.row_count_delta(max_percent_change=0.25, allow_decrease=True)
        d = c.to_dict()
        assert d["max_percent_change"] == 0.25
        assert d["allow_decrease"] is True

    def test_generic_metric(self):
        c = check.metric("accuracy", min=0.95)
        d = c.to_dict()
        assert d["type"] == "metric"
        assert d["metric_name"] == "accuracy"
        assert d["min"] == 0.95

    def test_generic_metric_range(self):
        c = check.metric("latency_ms", max=500.0)
        d = c.to_dict()
        assert d["metric_name"] == "latency_ms"
        assert d["max"] == 500.0

    def test_custom_assertion(self):
        c = check.custom("no_orphans")
        d = c.to_dict()
        assert d["type"] == "custom"
        assert d["assertion_name"] == "no_orphans"

    def test_severity_modifier(self):
        c = check.row_count(min=1).warning()
        assert c.severity == "warning"
        d = c.to_dict()
        assert d["severity"] == "warning"

    def test_name_modifier(self):
        c = check.row_count(min=1).named("minimum_rows")
        assert c.name == "minimum_rows"

    def test_description_modifier(self):
        c = check.row_count(min=1).described("We need at least one row")
        d = c.to_dict()
        assert d["description"] == "We need at least one row"


class TestContractDecorator:
    """Tests for the @contract decorator."""

    def test_attaches_contracts(self):
        @contract(
            check.row_count(min=1),
            check.unique("id"),
        )
        def my_task():
            pass

        assert hasattr(my_task, "_conduit_contracts")
        assert len(my_task._conduit_contracts) == 2

    def test_multiple_decorators_append(self):
        @contract(check.freshness("24h"))
        @contract(check.row_count(min=1))
        def my_task():
            pass

        assert len(my_task._conduit_contracts) == 2

    def test_preserves_function(self):
        @contract(check.row_count(min=1))
        def my_task():
            return 42

        assert my_task() == 42


class TestContractsBuilder:
    """Tests for the imperative Contracts builder."""

    def test_chaining(self):
        c = (
            Contracts("my_task")
            .row_count(min=1, max=100000)
            .freshness("24h")
            .unique(["id"])
            .not_null("email")
        )
        assert len(c._checks) == 4

    def test_generic_metric(self):
        c = Contracts("my_task").metric("accuracy", min=0.95)
        assert len(c._checks) == 1
        assert c._checks[0].type == "metric"

    def test_custom_assertion(self):
        c = Contracts("my_task").custom("no_orphans")
        assert len(c._checks) == 1
        assert c._checks[0].type == "custom"

    def test_to_dict_list(self):
        c = Contracts("task").row_count(min=1)
        dl = c.to_dict_list()
        assert len(dl) == 1
        assert dl[0]["type"] == "row_count"

    def test_emit(self, capsys):
        c = Contracts("my_task").row_count(min=1)
        c.emit()
        captured = capsys.readouterr()
        assert captured.out.startswith("CONDUIT::CONTRACT::")
        payload = json.loads(captured.out.strip().replace("CONDUIT::CONTRACT::", ""))
        assert payload["task_id"] == "my_task"
        assert len(payload["checks"]) == 1


class TestEvidenceEmission:
    """Tests for the evidence emission helpers."""

    def test_emit_metric(self, capsys):
        emit_metric("row_count", 5000)
        captured = capsys.readouterr()
        assert captured.out.strip() == "CONDUIT::METRIC::row_count::5000"

    def test_emit_evidence(self, capsys):
        emit_evidence({"row_count": 100, "accuracy": 0.95})
        captured = capsys.readouterr()
        lines = captured.out.strip().split("\n")
        assert len(lines) == 2
        assert "CONDUIT::METRIC::row_count::100" in lines[0]
        assert "CONDUIT::METRIC::accuracy::0.95" in lines[1]

    def test_emit_row_count(self, capsys):
        emit_row_count(42)
        captured = capsys.readouterr()
        assert "CONDUIT::METRIC::row_count::42" in captured.out

    def test_emit_freshness_seconds(self, capsys):
        emit_freshness_seconds(3600.0)
        captured = capsys.readouterr()
        assert "CONDUIT::METRIC::data_age_seconds::3600.0" in captured.out

    def test_emit_freshness_from_timestamp(self, capsys):
        from datetime import datetime, timezone, timedelta
        recent = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
        emit_freshness(recent)
        captured = capsys.readouterr()
        assert "CONDUIT::METRIC::data_age_seconds::" in captured.out

    def test_emit_duplicate_count(self, capsys):
        emit_duplicate_count(0)
        captured = capsys.readouterr()
        assert "CONDUIT::METRIC::duplicate_count::0" in captured.out

    def test_emit_null_rate(self, capsys):
        emit_null_rate("email", 0.02)
        captured = capsys.readouterr()
        assert "CONDUIT::METRIC::null_rate.email::0.02" in captured.out

    def test_emit_custom_pass(self, capsys):
        emit_custom("no_orphans", True)
        captured = capsys.readouterr()
        assert "CONDUIT::METRIC::pass.no_orphans::1.0" in captured.out

    def test_emit_custom_fail(self, capsys):
        emit_custom("no_orphans", False)
        captured = capsys.readouterr()
        assert "CONDUIT::METRIC::pass.no_orphans::0.0" in captured.out


class TestRuntimeAssertions:
    """Tests for runtime assertion helpers."""

    def test_row_count_passes(self, capsys):
        assert_row_count(100, min=1, max=1000)
        captured = capsys.readouterr()
        assert "CONDUIT::METRIC::row_count::100" in captured.out

    def test_row_count_min_fails(self):
        with pytest.raises(ValueError, match="at least"):
            assert_row_count(0, min=1)

    def test_row_count_max_fails(self):
        with pytest.raises(ValueError, match="at most"):
            assert_row_count(1001, max=1000)

    def test_row_count_exact_fails(self):
        with pytest.raises(ValueError, match="exactly"):
            assert_row_count(99, exact=100)

    def test_unique_passes(self, capsys):
        assert_unique([1, 2, 3, 4])
        captured = capsys.readouterr()
        assert "CONDUIT::METRIC::duplicate_count::0" in captured.out

    def test_unique_fails(self):
        with pytest.raises(ValueError, match="duplicate"):
            assert_unique([1, 2, 3, 2, 1])

    def test_freshness_passes(self, capsys):
        from datetime import datetime, timezone, timedelta
        recent = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
        assert_freshness(recent, "24h")
        captured = capsys.readouterr()
        assert "CONDUIT::METRIC::data_age_seconds::" in captured.out

    def test_freshness_fails(self):
        with pytest.raises(ValueError, match="old"):
            assert_freshness("2020-01-01T00:00:00Z", "24h")
