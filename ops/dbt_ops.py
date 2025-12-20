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

    # Resolve env vars AT RUNTIME (critical)
    dbt_bin = os.getenv("DBT_BIN", "dbt")

    dbt_project_dir = Path(
        os.getenv(
            "DBT_PROJECT_DIR",
            str(Path.home() / "dbt_stockmarketproject"),
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

    if not dbt_project_dir.exists():
        raise FileNotFoundError(f"DBT project dir not found: {dbt_project_dir}")

    if not dbt_profiles_dir.exists():
        raise FileNotFoundError(f"DBT profiles dir not found: {dbt_profiles_dir}")

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

    logger.info(result.stdout)

    if result.returncode != 0:
        logger.error(result.stderr)
        raise RuntimeError("dbt build failed")

    logger.info("dbt build completed successfully")
