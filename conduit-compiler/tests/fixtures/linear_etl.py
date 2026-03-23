from conduit_sdk import dag, task

@dag(schedule="0 6 * * *", tags=["etl", "warehouse"])
def linear_etl():
    """A linear 3-task ETL pipeline."""

    @task(retries=3, retry_delay="5m", pool="source_pool")
    def extract_orders():
        """Extract orders from source database."""
        pass

    @task(pool="transform_pool", timeout="30m")
    def transform_orders(raw):
        """Clean and normalize order data."""
        pass

    @task(pool="warehouse_pool", priority=10)
    def load_orders(data):
        """Load transformed orders into the warehouse."""
        pass

    raw = extract_orders()
    cleaned = transform_orders(raw)
    load_orders(cleaned)
