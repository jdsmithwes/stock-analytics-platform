from dagster import job
from ops.dbt_ops import run_dbt_build

@job
def dbt_job():
    run_dbt_build()