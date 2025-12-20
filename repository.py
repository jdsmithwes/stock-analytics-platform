from dagster import repository
from jobs import dbt_job
from schedules import nightly_dbt_schedule

@repository
def dbt_stockmarket_repo():
    return [
        dbt_job,
        nightly_dbt_schedule,
    ]
