from dagster import run_status_sensor, DagsterRunStatus
from notifications.sns import send_sns


@run_status_sensor(
    run_status=DagsterRunStatus.SUCCESS,
    request_job=None,
)
def dagster_job_success_sensor(context):
    run = context.dagster_run

    send_sns(
        subject=f"✅ Dagster Job Succeeded: {run.job_name}",
        message=f"""
Dagster job completed successfully.

Job: {run.job_name}
Run ID: {run.run_id}
""",
    )


@run_status_sensor(
    run_status=DagsterRunStatus.FAILURE,
    request_job=None,
)
def dagster_job_failure_sensor(context):
    run = context.dagster_run

    send_sns(
        subject=f"❌ Dagster Job FAILED: {run.job_name}",
        message=f"""
Dagster job FAILED.

Job: {run.job_name}
Run ID: {run.run_id}

Check Dagster UI for logs.
""",
    )

