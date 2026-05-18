"""Cross-task lineage demo: Python declarative model.

This file demonstrates Conduit's *declarative* lineage API — explicit
``@task(inputs=[Dataset(...)], outputs=[Dataset(...)])`` annotations
plus ``lineage_strict=True`` on the DAG. The stitcher links matched
column names across task boundaries; trace from any output column back
through the upstream task(s) that declared it.

    conduit lineage trace \\
        --dag cross_task_lineage_demo \\
        --column transform.total \\
        --dags-path examples/dags

For the *implementation-aware* variant — where the middle task's SQL
``SUM(amount) AS total`` is parsed and the lineage trace from
``load.total`` reaches ``seed.amount`` — see the sibling
``cross_task_lineage.yaml``. Python tasks need a column-mapping
declaration to express the same intra-task flow (planned).
"""

from conduit_sdk import ColumnSpec, Dataset, dag, task


@dag(
    schedule="@daily",
    tags=["lineage", "demo"],
    lineage_strict=True,
)
def cross_task_lineage_demo():
    @task(
        outputs=[
            Dataset(
                "staging.orders",
                columns=[
                    ColumnSpec("id"),
                    ColumnSpec("customer_id"),
                    ColumnSpec("amount", dtype="DECIMAL"),
                ],
            )
        ],
    )
    def extract_orders():
        """Pull raw orders from the source system into the staging layer."""

    @task(
        inputs=[
            Dataset(
                "staging.orders",
                columns=[
                    ColumnSpec("customer_id"),
                    ColumnSpec("amount", dtype="DECIMAL"),
                ],
            )
        ],
        outputs=[
            Dataset(
                "analytics.daily_revenue",
                columns=[
                    ColumnSpec("customer_id"),
                    ColumnSpec("total", dtype="DECIMAL"),
                ],
            )
        ],
    )
    def transform(orders=extract_orders):
        """Aggregate per-customer revenue. In a real pipeline this is the
        place a SQL warehouse task or a dbt model would sit; the lineage
        story is identical either way."""

    @task(
        inputs=[
            Dataset(
                "analytics.daily_revenue",
                columns=[
                    ColumnSpec("customer_id"),
                    ColumnSpec("total", dtype="DECIMAL"),
                ],
            )
        ],
    )
    def push_to_warehouse(revenue=transform):
        """Ship the aggregated rows to the downstream BI warehouse."""
