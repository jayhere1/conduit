from conduit import dag, task, Param

@dag(
    schedule="0 2 * * *",
    tags=["ml", "training"],
    max_active_runs=1,
)
def daily_model_training(date: Param[str] = "{{ ds }}"):
    """Train and evaluate ML models on daily data."""

    @task(retries=2, pool="snowflake", timeout="20m")
    def extract_training_data(date: str):
        """Pull labeled training data from the warehouse."""
        pass

    @task(timeout="10m")
    def validate_data(raw_data):
        """Run data quality checks on training data."""
        pass

    @task(pool="gpu", timeout="2h")
    def train_model(validated_data):
        """Train the ML model on validated data."""
        pass

    @task(timeout="30m")
    def evaluate_model(model):
        """Evaluate model performance on holdout set."""
        pass

    @task()
    def compare_with_production(evaluation):
        """Compare new model metrics against production model."""
        pass

    @task()
    def publish_model(comparison):
        """Publish model to the model registry if metrics improved."""
        pass

    @task()
    def notify_ml_team(result):
        """Send model training results to the ML team."""
        pass

    data = extract_training_data(date)
    validated = validate_data(data)
    model = train_model(validated)
    evaluation = evaluate_model(model)
    comparison = compare_with_production(evaluation)
    publish_model(comparison)
    notify_ml_team(comparison)
