from dagster import job
from dagster_pipeline.ops.dbt_ops import run_dbt_build

@job
def dbt_job():
    run_dbt_build()
