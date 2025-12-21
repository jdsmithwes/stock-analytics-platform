from dagster import schedule
from jobs import dbt_job

@schedule(
    cron_schedule="30 23 * * *",
    execution_timezone="US/Eastern",
    job=dbt_job,
)
def nightly_dbt_schedule():
    return {}
