from dagster import repository
from jobs import dbt_job
from schedules import nightly_dbt_schedule
from sensors.run_status_sensors import (
    dagster_job_success_sensor,
    dagster_job_failure_sensor,
)

@repository
def dbt_stockmarket_repo():
    return [
        dbt_job,
        nightly_dbt_schedule,
        dagster_job_success_sensor,
        dagster_job_failure_sensor,
    ]