from dagster import Definitions

from dagster_pipeline.jobs import dbt_job
from dagster_pipeline.schedules import nightly_dbt_schedule
from dagster_pipeline.sensors.run_status_sensors import (
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

