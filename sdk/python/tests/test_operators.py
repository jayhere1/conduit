"""Tests for Conduit operators and hooks."""

import json
import os
import sqlite3
import tempfile
import threading
import time
from unittest.mock import patch, MagicMock

import pytest

from conduit_sdk.operators.python_operator import PythonOperator
from conduit_sdk.operators.bash_operator import BashOperator
from conduit_sdk.operators.sql_operator import SQLOperator
from conduit_sdk.operators.sensor import Sensor, FileSensor, HttpSensor, SqlSensor
from conduit_sdk.operators.notifications import SlackNotifyOperator, EmailOperator
from conduit_sdk.operators.base import BaseOperator, _parse_duration
from conduit_sdk.hooks.base import BaseHook, Connection
from conduit_sdk.hooks.database import DatabaseHook
from conduit_sdk.hooks.http import HttpHook, HttpResponse
from conduit_sdk.hooks.filesystem import FileSystemHook


# ─── Duration Parsing ───────────────────────────────────────────────────

class TestParseDuration:
    def test_seconds(self):
        assert _parse_duration("30s") == 30.0

    def test_minutes(self):
        assert _parse_duration("5m") == 300.0

    def test_hours(self):
        assert _parse_duration("2h") == 7200.0

    def test_days(self):
        assert _parse_duration("1d") == 86400.0

    def test_composite(self):
        assert _parse_duration("1h30m") == 5400.0

    def test_numeric_string(self):
        assert _parse_duration("42") == 42.0

    def test_empty(self):
        assert _parse_duration("") == 0.0

    def test_invalid(self):
        with pytest.raises(ValueError, match="Cannot parse duration"):
            _parse_duration("invalid")


# ─── Connection Parsing ─────────────────────────────────────────────────

class TestConnectionParsing:
    def test_from_json(self):
        raw = json.dumps({
            "conn_type": "postgres",
            "host": "db.example.com",
            "port": 5432,
            "schema": "analytics",
            "login": "user",
            "password": "secret",
            "extra": {"sslmode": "require"},
        })
        conn = Connection.from_json("my_pg", raw)
        assert conn.conn_id == "my_pg"
        assert conn.conn_type == "postgres"
        assert conn.host == "db.example.com"
        assert conn.port == 5432
        assert conn.schema == "analytics"
        assert conn.login == "user"
        assert conn.password == "secret"
        assert conn.extra == {"sslmode": "require"}

    def test_from_json_extra_as_string(self):
        raw = json.dumps({
            "conn_type": "postgres",
            "host": "localhost",
            "extra": '{"sslmode": "disable"}',
        })
        conn = Connection.from_json("test", raw)
        assert conn.extra == {"sslmode": "disable"}

    def test_from_json_minimal(self):
        raw = json.dumps({"conn_type": "sqlite"})
        conn = Connection.from_json("lite", raw)
        assert conn.conn_type == "sqlite"
        assert conn.host == ""
        assert conn.port == 0

    def test_from_json_invalid(self):
        with pytest.raises(ValueError, match="Cannot parse connection JSON"):
            Connection.from_json("bad", "not json{")

    def test_from_uri_postgres(self):
        conn = Connection.from_uri("pg", "postgres://user:pass@localhost:5432/mydb")
        assert conn.conn_id == "pg"
        assert conn.conn_type == "postgres"
        assert conn.host == "localhost"
        assert conn.port == 5432
        assert conn.schema == "mydb"
        assert conn.login == "user"
        assert conn.password == "pass"

    def test_from_uri_postgresql(self):
        conn = Connection.from_uri("pg", "postgresql://u:p@host/db")
        assert conn.conn_type == "postgres"

    def test_from_uri_mysql(self):
        conn = Connection.from_uri("my", "mysql://root:pw@db:3306/app?charset=utf8")
        assert conn.conn_type == "mysql"
        assert conn.port == 3306
        assert conn.extra == {"charset": "utf8"}

    def test_from_uri_sqlite(self):
        conn = Connection.from_uri("lite", "sqlite:///path/to/db.sqlite")
        assert conn.conn_type == "sqlite"
        assert conn.schema == "path/to/db.sqlite"

    def test_from_uri_http(self):
        conn = Connection.from_uri("api", "http://token@api.example.com/v1")
        assert conn.conn_type == "http"
        assert conn.host == "api.example.com"
        assert conn.login == "token"
        assert conn.schema == "v1"

    def test_env_var_resolution_json(self, monkeypatch):
        raw = json.dumps({"conn_type": "postgres", "host": "db.local", "port": 5432})
        monkeypatch.setenv("CONDUIT_CONN_MY_DB", raw)
        hook = BaseHook("my_db")
        conn = hook.get_connection()
        assert conn.conn_type == "postgres"
        assert conn.host == "db.local"

    def test_env_var_resolution_uri(self, monkeypatch):
        monkeypatch.setenv("CONDUIT_CONN_MY_PG", "postgres://u:p@host:5432/db")
        hook = BaseHook("my_pg")
        conn = hook.get_connection()
        assert conn.conn_type == "postgres"
        assert conn.host == "host"

    def test_env_var_missing(self):
        # Ensure the env var is not set
        os.environ.pop("CONDUIT_CONN_NONEXISTENT", None)
        hook = BaseHook("nonexistent")
        with pytest.raises(ValueError, match="not found"):
            hook.get_connection()

    def test_connection_caching(self, monkeypatch):
        monkeypatch.setenv("CONDUIT_CONN_CACHED", '{"conn_type": "test"}')
        hook = BaseHook("cached")
        conn1 = hook.get_connection()
        conn2 = hook.get_connection()
        assert conn1 is conn2


# ─── BashOperator ───────────────────────────────────────────────────────

class TestBashOperator:
    def test_simple_command(self):
        op = BashOperator(task_id="echo_test", bash_command="echo hello world")
        result = op.run()
        assert result == "hello world"

    def test_command_captures_stdout(self):
        op = BashOperator(task_id="multi_line", bash_command="echo line1; echo line2")
        result = op.run()
        assert "line1" in result
        assert "line2" in result

    def test_command_failure(self):
        op = BashOperator(task_id="fail_test", bash_command="exit 1")
        with pytest.raises(Exception):
            op.run()

    def test_custom_env(self):
        op = BashOperator(
            task_id="env_test",
            bash_command="echo $MY_VAR",
            env={"MY_VAR": "custom_value"},
        )
        result = op.run()
        assert result == "custom_value"

    def test_custom_cwd(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            op = BashOperator(
                task_id="cwd_test",
                bash_command="pwd",
                cwd=tmpdir,
            )
            result = op.run()
            # On macOS /tmp -> /private/tmp, so use realpath
            assert os.path.realpath(result) == os.path.realpath(tmpdir)

    def test_template_rendering(self):
        from conduit_sdk.context import TaskContext
        from datetime import datetime

        ctx = TaskContext(
            dag_id="test_dag",
            task_id="test_task",
            run_id="run_123",
            attempt=1,
            logical_date=datetime(2026, 3, 23),
            environment="test",
            upstream_xcom={},
        )
        rendered = BashOperator._render_templates(
            "echo {{ dag_id }} {{ ds }} {{ run_id }}", ctx
        )
        assert rendered == "echo test_dag 2026-03-23 run_123"

    def test_template_unknown_variable_unchanged(self):
        from conduit_sdk.context import TaskContext

        ctx = TaskContext(
            dag_id="d", task_id="t", run_id="r",
            attempt=1, logical_date=None,
            environment="test", upstream_xcom={},
        )
        rendered = BashOperator._render_templates("{{ unknown_var }}", ctx)
        assert rendered == "{{ unknown_var }}"

    def test_retries(self):
        """BashOperator retries the configured number of times."""
        call_count = {"n": 0}
        orig_run = BashOperator.execute

        def patched_execute(self, context=None):
            call_count["n"] += 1
            if call_count["n"] < 3:
                raise RuntimeError("transient failure")
            return orig_run(self, context)

        op = BashOperator(
            task_id="retry_test",
            bash_command="echo ok",
            retries=2,
            retry_delay="0",
        )
        with patch.object(BashOperator, "execute", patched_execute):
            result = op.run()
        assert result == "ok"
        assert call_count["n"] == 3


# ─── PythonOperator ─────────────────────────────────────────────────────

class TestPythonOperator:
    def test_simple_callable(self):
        def add(a, b):
            return a + b

        op = PythonOperator(
            task_id="add_test",
            python_callable=add,
            op_args=[3, 4],
        )
        result = op.run()
        assert result == 7

    def test_kwargs(self):
        def greet(name, greeting="Hello"):
            return f"{greeting}, {name}!"

        op = PythonOperator(
            task_id="greet_test",
            python_callable=greet,
            op_args=["World"],
            op_kwargs={"greeting": "Hi"},
        )
        result = op.run()
        assert result == "Hi, World!"

    def test_context_injection(self):
        def task_with_context(context=None):
            return context.dag_id

        op = PythonOperator(
            task_id="ctx_test",
            python_callable=task_with_context,
        )
        result = op.run()
        assert isinstance(result, str)

    def test_kwargs_context_injection(self):
        def task_with_kwargs(**kwargs):
            return kwargs.get("context") is not None

        op = PythonOperator(
            task_id="kwargs_ctx_test",
            python_callable=task_with_kwargs,
        )
        result = op.run()
        assert result is True

    def test_return_none(self):
        def noop():
            pass

        op = PythonOperator(task_id="noop_test", python_callable=noop)
        result = op.run()
        assert result is None

    def test_not_callable_raises(self):
        with pytest.raises(TypeError, match="must be callable"):
            PythonOperator(task_id="bad", python_callable="not a function")

    def test_captures_return_value(self, capsys):
        def produce():
            return {"key": "value"}

        op = PythonOperator(task_id="capture_test", python_callable=produce)
        result = op.run()
        assert result == {"key": "value"}

        # Verify XCom protocol message was emitted
        captured = capsys.readouterr()
        assert "CONDUIT::XCOM::return_value=" in captured.out


# ─── SQLOperator (SQLite) ───────────────────────────────────────────────

class TestSQLOperator:
    @pytest.fixture
    def sqlite_db(self, tmp_path, monkeypatch):
        """Set up a SQLite database with test data."""
        db_path = str(tmp_path / "test.db")

        # Create table and insert data
        conn = sqlite3.connect(db_path)
        conn.execute("CREATE TABLE orders (id INTEGER, name TEXT, total REAL)")
        conn.execute("INSERT INTO orders VALUES (1, 'Alice', 100.0)")
        conn.execute("INSERT INTO orders VALUES (2, 'Bob', 200.0)")
        conn.execute("INSERT INTO orders VALUES (3, 'Charlie', 150.0)")
        conn.commit()
        conn.close()

        # Set up the connection env var
        monkeypatch.setenv(
            "CONDUIT_CONN_TEST_SQLITE",
            json.dumps({"conn_type": "sqlite", "host": db_path}),
        )
        return db_path

    def test_select_query(self, sqlite_db):
        op = SQLOperator(
            task_id="select_test",
            sql="SELECT * FROM orders ORDER BY id",
            connection_id="test_sqlite",
        )
        rows = op.run()
        assert len(rows) == 3
        assert rows[0] == (1, "Alice", 100.0)

    def test_parameterized_query(self, sqlite_db):
        op = SQLOperator(
            task_id="param_test",
            sql="SELECT name FROM orders WHERE total > ?",
            connection_id="test_sqlite",
            parameters=(120.0,),
        )
        rows = op.run()
        assert len(rows) == 2
        names = [r[0] for r in rows]
        assert "Bob" in names
        assert "Charlie" in names

    def test_insert_returns_empty(self, sqlite_db):
        op = SQLOperator(
            task_id="insert_test",
            sql="INSERT INTO orders VALUES (4, 'Diana', 300.0)",
            connection_id="test_sqlite",
        )
        rows = op.run()
        assert rows == []

    def test_emits_row_count_metric(self, sqlite_db, capsys):
        op = SQLOperator(
            task_id="metric_test",
            sql="SELECT * FROM orders",
            connection_id="test_sqlite",
        )
        op.run()
        captured = capsys.readouterr()
        assert "CONDUIT::METRIC::row_count=" in captured.out

    def test_aggregate_query(self, sqlite_db):
        op = SQLOperator(
            task_id="agg_test",
            sql="SELECT count(*), sum(total) FROM orders",
            connection_id="test_sqlite",
        )
        rows = op.run()
        assert rows[0][0] == 3
        assert rows[0][1] == 450.0


# ─── FileSensor ─────────────────────────────────────────────────────────

class TestFileSensor:
    def test_file_already_exists(self, tmp_path):
        f = tmp_path / "data.csv"
        f.write_text("a,b,c")

        sensor = FileSensor(
            task_id="file_exists_test",
            path=str(f),
            poke_interval="0",
            timeout="5s",
        )
        result = sensor.run()
        assert result is True

    def test_file_created_during_wait(self, tmp_path):
        f = tmp_path / "delayed.csv"

        def create_file():
            time.sleep(0.3)
            f.write_text("data")

        t = threading.Thread(target=create_file)
        t.start()

        sensor = FileSensor(
            task_id="file_wait_test",
            path=str(f),
            poke_interval="0.1s",
            timeout="5s",
        )
        result = sensor.run()
        t.join()
        assert result is True

    def test_file_timeout(self, tmp_path):
        sensor = FileSensor(
            task_id="file_timeout_test",
            path=str(tmp_path / "nonexistent.csv"),
            poke_interval="0.05s",
            timeout="0.2s",
        )
        with pytest.raises(TimeoutError, match="timed out"):
            sensor.run()


# ─── SqlSensor (SQLite) ─────────────────────────────────────────────────

class TestSqlSensor:
    def test_sql_sensor_with_data(self, tmp_path, monkeypatch):
        db_path = str(tmp_path / "sensor.db")
        conn = sqlite3.connect(db_path)
        conn.execute("CREATE TABLE events (id INTEGER)")
        conn.execute("INSERT INTO events VALUES (1)")
        conn.commit()
        conn.close()

        monkeypatch.setenv(
            "CONDUIT_CONN_SENSOR_DB",
            json.dumps({"conn_type": "sqlite", "host": db_path}),
        )

        sensor = SqlSensor(
            task_id="sql_sensor_test",
            sql="SELECT 1 FROM events LIMIT 1",
            connection_id="sensor_db",
            poke_interval="0",
            timeout="5s",
        )
        assert sensor.run() is True

    def test_sql_sensor_timeout_no_data(self, tmp_path, monkeypatch):
        db_path = str(tmp_path / "empty.db")
        conn = sqlite3.connect(db_path)
        conn.execute("CREATE TABLE events (id INTEGER)")
        conn.commit()
        conn.close()

        monkeypatch.setenv(
            "CONDUIT_CONN_EMPTY_DB",
            json.dumps({"conn_type": "sqlite", "host": db_path}),
        )

        sensor = SqlSensor(
            task_id="sql_empty_test",
            sql="SELECT 1 FROM events LIMIT 1",
            connection_id="empty_db",
            poke_interval="0.05s",
            timeout="0.2s",
        )
        with pytest.raises(TimeoutError, match="timed out"):
            sensor.run()


# ─── DatabaseHook (SQLite) ──────────────────────────────────────────────

class TestDatabaseHook:
    def test_sqlite_connection(self, tmp_path, monkeypatch):
        db_path = str(tmp_path / "hook_test.db")
        monkeypatch.setenv(
            "CONDUIT_CONN_HOOK_DB",
            json.dumps({"conn_type": "sqlite", "host": db_path}),
        )

        hook = DatabaseHook("hook_db")
        db_conn = hook.get_conn()
        assert db_conn is not None
        db_conn.close()

    def test_sqlite_run(self, tmp_path, monkeypatch):
        db_path = str(tmp_path / "run_test.db")
        conn = sqlite3.connect(db_path)
        conn.execute("CREATE TABLE t (val TEXT)")
        conn.execute("INSERT INTO t VALUES ('hello')")
        conn.commit()
        conn.close()

        monkeypatch.setenv(
            "CONDUIT_CONN_RUN_DB",
            json.dumps({"conn_type": "sqlite", "host": db_path}),
        )

        hook = DatabaseHook("run_db")
        rows = hook.run("SELECT val FROM t")
        assert rows == [("hello",)]

    def test_unsupported_type(self, monkeypatch):
        monkeypatch.setenv(
            "CONDUIT_CONN_BAD_DB",
            json.dumps({"conn_type": "oracle", "host": "localhost"}),
        )
        hook = DatabaseHook("bad_db")
        with pytest.raises(ValueError, match="Unsupported database"):
            hook.get_conn()


# ─── HttpHook ───────────────────────────────────────────────────────────

class TestHttpHook:
    def test_http_response_wrapper(self):
        resp = HttpResponse(200, '{"ok": true}', {"Content-Type": "application/json"})
        assert resp.status_code == 200
        assert resp.json() == {"ok": True}
        assert repr(resp) == "<HttpResponse [200]>"

    def test_http_response_raise_for_status(self):
        resp = HttpResponse(404, "Not Found")
        with pytest.raises(RuntimeError, match="HTTP 404"):
            resp.raise_for_status()

    def test_http_response_raise_for_status_ok(self):
        resp = HttpResponse(200, "OK")
        resp.raise_for_status()  # Should not raise


# ─── FileSystemHook ─────────────────────────────────────────────────────

class TestFileSystemHook:
    def test_get_path(self, tmp_path, monkeypatch):
        monkeypatch.setenv(
            "CONDUIT_CONN_DATA_DIR",
            json.dumps({"conn_type": "filesystem", "host": str(tmp_path)}),
        )
        hook = FileSystemHook("data_dir")
        assert hook.get_path() == str(tmp_path)

    def test_list_files(self, tmp_path, monkeypatch):
        (tmp_path / "a.csv").write_text("data")
        (tmp_path / "b.csv").write_text("data")
        (tmp_path / "c.txt").write_text("text")

        monkeypatch.setenv(
            "CONDUIT_CONN_FILES",
            json.dumps({"conn_type": "filesystem", "host": str(tmp_path)}),
        )
        hook = FileSystemHook("files")
        csv_files = hook.list_files("*.csv")
        assert len(csv_files) == 2
        assert all(f.endswith(".csv") for f in csv_files)

    def test_exists(self, tmp_path, monkeypatch):
        (tmp_path / "exists.txt").write_text("yes")
        monkeypatch.setenv(
            "CONDUIT_CONN_FS",
            json.dumps({"conn_type": "filesystem", "host": str(tmp_path)}),
        )
        hook = FileSystemHook("fs")
        assert hook.exists("exists.txt") is True
        assert hook.exists("nope.txt") is False

    def test_no_path_raises(self, monkeypatch):
        monkeypatch.setenv(
            "CONDUIT_CONN_EMPTY_FS",
            json.dumps({"conn_type": "filesystem"}),
        )
        hook = FileSystemHook("empty_fs")
        with pytest.raises(ValueError, match="no path configured"):
            hook.get_path()


# ─── BaseOperator ───────────────────────────────────────────────────────

class TestBaseOperator:
    def test_repr(self):
        op = BashOperator(task_id="my_task", bash_command="echo hi")
        assert "BashOperator" in repr(op)
        assert "my_task" in repr(op)

    def test_default_kwargs(self):
        op = BashOperator(task_id="defaults", bash_command="true")
        assert op.retries == 0
        assert op.pool is None
        assert op.timeout is None
        assert op.priority == 0
        assert op.trigger_rule == "all_success"
        assert op.tags == []

    def test_custom_kwargs(self):
        op = BashOperator(
            task_id="custom",
            bash_command="true",
            retries=3,
            retry_delay="1m",
            pool="my_pool",
            timeout="30m",
            priority=5,
            tags=["important"],
        )
        assert op.retries == 3
        assert op.retry_delay == "1m"
        assert op.pool == "my_pool"
        assert op.timeout == "30m"
        assert op.priority == 5
        assert op.tags == ["important"]

    def test_execute_not_implemented(self):
        op = BaseOperator(task_id="base")
        with pytest.raises(NotImplementedError):
            op.execute()


# ─── Sensor Base ────────────────────────────────────────────────────────

class TestSensorBase:
    def test_poke_not_implemented(self):
        sensor = Sensor(task_id="base_sensor")
        with pytest.raises(NotImplementedError):
            sensor.poke()

    def test_custom_sensor(self):
        class CountSensor(Sensor):
            def __init__(self, target, **kwargs):
                super().__init__(**kwargs)
                self.count = 0
                self.target = target

            def poke(self, context=None):
                self.count += 1
                return self.count >= self.target

        sensor = CountSensor(
            task_id="count_sensor",
            target=3,
            poke_interval="0",
            timeout="5s",
        )
        result = sensor.run()
        assert result is True
        assert sensor.count == 3


# ─── Template Variable Substitution (additional coverage) ───────────────

class TestTemplateSubstitution:
    def test_ds_nodash(self):
        from conduit_sdk.context import TaskContext
        from datetime import datetime

        ctx = TaskContext(
            dag_id="d", task_id="t", run_id="r",
            attempt=2,
            logical_date=datetime(2026, 1, 15),
            environment="prod",
            upstream_xcom={},
        )
        rendered = BashOperator._render_templates(
            "echo {{ ds_nodash }} attempt={{ attempt }}", ctx
        )
        assert rendered == "echo 20260115 attempt=2"

    def test_no_context(self):
        rendered = BashOperator._render_templates("echo {{ ds }}", None)
        assert rendered == "echo {{ ds }}"

    def test_no_logical_date(self):
        from conduit_sdk.context import TaskContext

        ctx = TaskContext(
            dag_id="d", task_id="t", run_id="r",
            attempt=1, logical_date=None,
            environment="test", upstream_xcom={},
        )
        rendered = BashOperator._render_templates("echo {{ ds }}", ctx)
        assert rendered == "echo no-date"


# ─── Protocol Message Emission ──────────────────────────────────────────

class TestProtocolMessages:
    def test_bash_emits_log_messages(self, capsys):
        op = BashOperator(task_id="log_test", bash_command="echo hi")
        op.run()
        captured = capsys.readouterr()
        assert "CONDUIT::LOG::" in captured.out

    def test_python_emits_metric(self, capsys):
        def do_work():
            return 42

        op = PythonOperator(
            task_id="metric_emit_test",
            python_callable=do_work,
        )
        op.run()
        captured = capsys.readouterr()
        assert "CONDUIT::METRIC::duration_seconds=" in captured.out

    def test_sql_emits_row_count(self, tmp_path, monkeypatch, capsys):
        db_path = str(tmp_path / "proto.db")
        conn = sqlite3.connect(db_path)
        conn.execute("CREATE TABLE t (id INTEGER)")
        conn.execute("INSERT INTO t VALUES (1)")
        conn.execute("INSERT INTO t VALUES (2)")
        conn.commit()
        conn.close()

        monkeypatch.setenv(
            "CONDUIT_CONN_PROTO_DB",
            json.dumps({"conn_type": "sqlite", "host": db_path}),
        )

        op = SQLOperator(
            task_id="proto_sql",
            sql="SELECT * FROM t",
            connection_id="proto_db",
        )
        op.run()
        captured = capsys.readouterr()
        assert "CONDUIT::METRIC::row_count=" in captured.out
