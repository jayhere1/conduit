from conduit_sdk import dag, task

@dag(schedule="0 */4 * * *", tags=["analytics"], max_active_runs=2)
def diamond_dag():
    """Fan-out / fan-in pattern."""

    @task()
    def start():
        pass

    @task()
    def branch_left(data):
        pass

    @task()
    def branch_right(data):
        pass

    @task()
    def join(left, right):
        pass

    data = start()
    left = branch_left(data)
    right = branch_right(data)
    join(left, right)
