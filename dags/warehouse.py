from conduit import dag, task, Param

@dag(
    schedule="0 6 * * *",
    tags=["etl", "warehouse"],
    max_active_runs=3,
)
def daily_warehouse_refresh(date: Param[str] = "{{ ds }}"):
    """Refresh the data warehouse with daily order data."""

    @task(retries=3, retry_delay="5m", pool="snowflake")
    def extract_orders(date: str):
        """Pull orders from the source database."""
        pass

    @task(retries=3, retry_delay="5m", pool="snowflake")
    def extract_customers(date: str):
        """Pull customer data from the source database."""
        pass

    @task(pool="snowflake", timeout="30m")
    def transform_orders(raw_orders, customers):
        """Join orders with customers and apply business logic."""
        pass

    @task(pool="snowflake", timeout="15m")
    def aggregate_revenue(transformed):
        """Compute daily revenue aggregates."""
        pass

    @task(pool="snowflake")
    def load_to_warehouse(aggregated):
        """Load final data into the warehouse."""
        pass

    @task()
    def send_notification():
        """Notify the team that the refresh is complete."""
        pass

    orders = extract_orders(date)
    customers = extract_customers(date)
    transformed = transform_orders(orders, customers)
    aggregated = aggregate_revenue(transformed)
    load_to_warehouse(aggregated)
    send_notification()
