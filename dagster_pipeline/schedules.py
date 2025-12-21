from dagster import schedule
from dagster_pipeline.jobs import dbt_job

@schedule(
    cron_schedule="0 23 * * *",  # nightly at 11 PM
    job=dbt_job,
    execution_timezone="US/Eastern",
)
def nightly_dbt_schedule(_context):
    return {}
