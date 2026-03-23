from conduit import dag, task, sensor, Param

@dag(
    schedule="0 8 * * 1",
    tags=["marketing", "analytics"],
    max_active_runs=1,
)
def weekly_marketing_report(week: Param[str] = "{{ ds }}"):
    """Generate weekly marketing performance reports."""

    @task(retries=2, pool="bigquery")
    def extract_campaign_data(week: str):
        """Pull campaign performance from ad platforms."""
        pass

    @task(retries=2, pool="bigquery")
    def extract_web_analytics(week: str):
        """Pull web analytics from GA4."""
        pass

    @task(pool="bigquery", timeout="20m")
    def merge_attribution(campaigns, web_data):
        """Merge campaign data with web analytics for attribution."""
        pass

    @task(timeout="10m")
    def generate_report(attributed):
        """Generate the weekly PDF report."""
        pass

    @task()
    def email_stakeholders(report):
        """Email the report to marketing leadership."""
        pass

    campaigns = extract_campaign_data(week)
    web = extract_web_analytics(week)
    attributed = merge_attribution(campaigns, web)
    report = generate_report(attributed)
    email_stakeholders(report)
