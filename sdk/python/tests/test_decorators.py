"""Tests for the @dag and @task decorators."""

import pytest
from conduit_sdk import dag, task
from conduit_sdk.decorators import (
    DagDefinition,
    TaskDefinition,
    clear_registry,
    get_dag,
    list_dags,
)


@pytest.fixture(autouse=True)
def clean_registry():
    """Clear the global registry before each test."""
    clear_registry()
    yield
    clear_registry()


def test_basic_dag_definition():
    """@dag creates a DagDefinition with correct metadata."""

    @dag(schedule="0 6 * * *", tags=["etl"])
    def my_pipeline():
        @task()
        def step_one():
            pass

        @task()
        def step_two():
            pass

    assert isinstance(my_pipeline, DagDefinition)
    assert my_pipeline.id == "my_pipeline"
    assert my_pipeline.schedule == "0 6 * * *"
    assert my_pipeline.tags == ["etl"]
    assert len(my_pipeline.tasks) == 2
    assert "step_one" in my_pipeline.tasks
    assert "step_two" in my_pipeline.tasks


def test_task_metadata():
    """@task captures all keyword arguments."""

    @dag()
    def test_dag():
        @task(retries=3, retry_delay="5m", retry_backoff=2.0, pool="my_pool", timeout="30m", priority=10)
        def my_task():
            """A task with lots of config."""
            pass

    t = test_dag.tasks["my_task"]
    assert t.retries == 3
    assert t.retry_delay == "5m"
    assert t.retry_backoff == 2.0
    assert t.pool == "my_pool"
    assert t.timeout == "30m"
    assert t.priority == 10
    assert t.doc == "A task with lots of config."


def test_dependency_resolution():
    """Dependencies are resolved from function default arguments."""

    @dag()
    def dep_dag():
        @task()
        def extract():
            return [1, 2, 3]

        @task()
        def transform(data=extract):
            return [x * 2 for x in data]

        @task()
        def load(transformed=transform):
            pass

    assert dep_dag.tasks["extract"].dependencies == []
    assert dep_dag.tasks["transform"].dependencies == ["extract"]
    assert dep_dag.tasks["load"].dependencies == ["transform"]


def test_diamond_dependencies():
    """Diamond-shaped dependency graphs resolve correctly."""

    @dag()
    def diamond():
        @task()
        def source():
            pass

        @task()
        def branch_a(data=source):
            pass

        @task()
        def branch_b(data=source):
            pass

        @task()
        def merge(a=branch_a, b=branch_b):
            pass

    assert diamond.tasks["source"].dependencies == []
    assert diamond.tasks["branch_a"].dependencies == ["source"]
    assert diamond.tasks["branch_b"].dependencies == ["source"]
    assert set(diamond.tasks["merge"].dependencies) == {"branch_a", "branch_b"}


def test_registry():
    """DAGs are registered globally and can be retrieved."""

    @dag(schedule="@daily")
    def registered_dag():
        @task()
        def a_task():
            pass

    assert get_dag("registered_dag") is registered_dag
    assert len(list_dags()) == 1


def test_multiple_dags():
    """Multiple DAGs can be defined and are all registered."""

    @dag(tags=["etl"])
    def etl_pipeline():
        @task()
        def extract():
            pass

    @dag(tags=["ml"])
    def ml_pipeline():
        @task()
        def train():
            pass

    assert len(list_dags()) == 2
    assert get_dag("etl_pipeline").tags == ["etl"]
    assert get_dag("ml_pipeline").tags == ["ml"]


def test_task_callable():
    """Tasks can be called directly for local testing."""

    @dag()
    def callable_dag():
        @task()
        def add(a=None, b=None):
            if a is not None and b is not None:
                return a + b
            return 42

    result = callable_dag.tasks["add"](3, 4)
    assert result == 7


def test_docstring_extraction():
    """DAG and task docstrings are captured."""

    @dag()
    def documented():
        """This is the DAG docstring."""

        @task()
        def documented_task():
            """This is the task docstring."""
            pass

    assert documented.doc == "This is the DAG docstring."
    assert documented.tasks["documented_task"].doc == "This is the task docstring."
