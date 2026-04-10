from conduit_sdk import dag, task

@dag(schedule="0 6 * * *", tags=["sdk-e2e-test"])
def sdk_e2e_pipeline():
    """End-to-end test: Python SDK -> compiler -> scheduler -> executor."""

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
