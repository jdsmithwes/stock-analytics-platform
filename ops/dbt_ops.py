from dagster import op, get_dagster_logger, RetryPolicy
import subprocess
import os
from pathlib import Path


@op(
    retry_policy=RetryPolicy(max_retries=2, delay=30),
    tags={"tool": "dbt"},
)
def run_dbt_build():
    logger = get_dagster_logger()

    # Resolve dbt binary at runtime
    dbt_bin = os.getenv("DBT_BIN", "dbt")

    # Correct nested dbt project directory
    dbt_project_dir = Path(
        os.getenv(
            "DBT_PROJECT_DIR",
            str(Path.home() / "dbt_stockmarket" / "dbt_stockmarket"),
        )
    )

    dbt_profiles_dir = Path(
        os.getenv(
            "DBT_PROFILES_DIR",
            str(Path.home() / ".dbt"),
        )
    )

    logger.info(f"DBT_PROJECT_DIR resolved to: {dbt_project_dir}")
    logger.info(f"DBT_PROFILES_DIR resolved to: {dbt_profiles_dir}")

    # Fail fast if paths are wrong
    if not dbt_project_dir.exists():
        raise FileNotFoundError(
            f"DBT project directory not found: {dbt_project_dir}"
        )

    if not (dbt_project_dir / "dbt_project.yml").exists():
        raise FileNotFoundError(
            f"dbt_project.yml not found in {dbt_project_dir}"
        )

    if not dbt_profiles_dir.exists():
        raise FileNotFoundError(
            f"DBT profiles directory not found: {dbt_profiles_dir}"
        )

    cmd = [
        dbt_bin,
        "build",
        "--project-dir",
        str(dbt_project_dir),
        "--profiles-dir",
        str(dbt_profiles_dir),
    ]

    logger.info(f"Running command: {' '.join(cmd)}")

    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
    )

    if result.stdout:
        logger.info(result.stdout)

    if result.returncode != 0:
        if result.stderr:
            logger.error(result.stderr)

        raise RuntimeError(
            f"dbt build failed with exit code {result.returncode}"
        )

    logger.info("dbt build completed successfully")