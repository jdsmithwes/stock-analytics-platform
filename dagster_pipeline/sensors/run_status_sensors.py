from dagster import run_status_sensor, DagsterRunStatus
from dagster_pipeline.notifications.sns import send_sns


@run_status_sensor(run_status=DagsterRunStatus.SUCCESS)
def dagster_job_success_sensor(context):
    send_sns(
        subject="Dagster Job Succeeded",
        message=f"Dagster job {context.dagster_run.job_name} succeeded."
    )


@run_status_sensor(run_status=DagsterRunStatus.FAILURE)
def dagster_job_failure_sensor(context):
    send_sns(
        subject="Dagster Job Failed",
        message=f"Dagster job {context.dagster_run.job_name} failed."
    )
