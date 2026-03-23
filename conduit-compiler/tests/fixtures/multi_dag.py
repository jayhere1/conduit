from conduit_sdk import dag, task

@dag(schedule="@daily", tags=["ingest"])
def ingest_pipeline():
    """First DAG in the file."""

    @task()
    def fetch():
        pass

    @task()
    def store(data):
        pass

    data = fetch()
    store(data)


@dag(schedule="@hourly", tags=["monitor"])
def monitoring_pipeline():
    """Second DAG in the same file."""

    @task()
    def check_health():
        pass

    @task()
    def alert(status):
        pass

    status = check_health()
    alert(status)
