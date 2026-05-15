from conduit_sdk import dag, task


@dag(
    schedule="0 6 * * *",
    tags=["etl", "warehouse"],
    max_active_runs=3,
)
def daily_warehouse_refresh():
    """Refresh the data warehouse with daily order data."""

    @task(retries=3, retry_delay="5m", pool="snowflake")
    def extract_orders():
        """Pull orders from the source database."""
        pass

    @task(retries=3, retry_delay="5m", pool="snowflake")
    def extract_customers():
        """Pull customer data from the source database."""
        pass

    @task(pool="snowflake", timeout="30m")
    def transform_orders(raw_orders=extract_orders, customers=extract_customers):
        """Join orders with customers and apply business logic."""
        pass

    @task(pool="snowflake", timeout="15m")
    def aggregate_revenue(transformed=transform_orders):
        """Compute daily revenue aggregates."""
        pass

    @task(pool="snowflake")
    def load_to_warehouse(aggregated=aggregate_revenue):
        """Load final data into the warehouse."""
        pass

    @task()
    def send_notification(loaded=load_to_warehouse):
        """Notify the team that the refresh is complete."""
        pass
