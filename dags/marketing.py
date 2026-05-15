from conduit_sdk import dag, task


@dag(
    schedule="0 8 * * 1",
    tags=["marketing", "analytics"],
    max_active_runs=1,
)
def weekly_marketing_report():
    """Generate weekly marketing performance reports."""

    @task(retries=2, pool="bigquery")
    def extract_campaign_data():
        """Pull campaign performance from ad platforms."""
        pass

    @task(retries=2, pool="bigquery")
    def extract_web_analytics():
        """Pull web analytics from GA4."""
        pass

    @task(pool="bigquery", timeout="20m")
    def merge_attribution(campaigns=extract_campaign_data, web_data=extract_web_analytics):
        """Merge campaign data with web analytics for attribution."""
        pass

    @task(timeout="10m")
    def generate_report(attributed=merge_attribution):
        """Generate the weekly PDF report."""
        pass

    @task()
    def email_stakeholders(report=generate_report):
        """Email the report to marketing leadership."""
        pass
