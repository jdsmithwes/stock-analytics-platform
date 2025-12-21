from dagster import Definitions

from jobs import dbt_job
from schedules import nightly_dbt_schedule
from sensors.run_status_sensors import (
    dagster_job_success_sensor,
    dagster_job_failure_sensor,
)

defs = Definitions(
    jobs=[dbt_job],
    schedules=[nightly_dbt_schedule],
    sensors=[
        dagster_job_success_sensor,
        dagster_job_failure_sensor,
    ],
)
