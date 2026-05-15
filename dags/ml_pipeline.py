from conduit_sdk import dag, task


@dag(
    schedule="0 2 * * *",
    tags=["ml", "training"],
    max_active_runs=1,
)
def daily_model_training():
    """Train and evaluate ML models on daily data."""

    @task(retries=2, pool="snowflake", timeout="20m")
    def extract_training_data():
        """Pull labeled training data from the warehouse."""
        pass

    @task(timeout="10m")
    def validate_data(raw_data=extract_training_data):
        """Run data quality checks on training data."""
        pass

    @task(pool="gpu", timeout="2h")
    def train_model(validated_data=validate_data):
        """Train the ML model on validated data."""
        pass

    @task(timeout="30m")
    def evaluate_model(model=train_model):
        """Evaluate model performance on holdout set."""
        pass

    @task()
    def compare_with_production(evaluation=evaluate_model):
        """Compare new model metrics against production model."""
        pass

    @task()
    def publish_model(comparison=compare_with_production):
        """Publish model to the model registry if metrics improved."""
        pass

    @task()
    def notify_ml_team(result=compare_with_production):
        """Send model training results to the ML team."""
        pass
