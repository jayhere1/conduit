from conduit_sdk import dag, task

@dag(schedule="0 6 * * *", tags=["test"])
def etl_pipeline():
    """Test DAG for integration tests."""

    @task()
    def extract():
        pass

    @task()
    def transform(raw):
        pass

    @task()
    def load(data):
        pass

    raw = extract()
    data = transform(raw)
    load(data)
