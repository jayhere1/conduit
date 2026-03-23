from conduit_sdk import dag, task

@dag(schedule="0 6 * * *", tags=["test"])
def py_dag():
    """Python DAG for mixed compile test."""

    @task()
    def step_a():
        pass

    @task()
    def step_b(data):
        pass

    a = step_a()
    step_b(a)
