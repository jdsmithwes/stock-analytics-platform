from dagster import op, get_dagster_logger, RetryPolicy
import subprocess
import os
from pathlib import Path

DBT_BIN = os.getenv("DBT_BIN", "dbt")

DBT_PROJECT_DIR = Path(
    os.getenv(
        "DBT_PROJECT_DIR",
        str(Path.home() / "dbt_stockmarketproject"),
    )
)

DBT_PROFILES_DIR = Path(
    os.getenv(
        "DBT_PROFILES_DIR",
        str(Path.home() / ".dbt"),
    )
)

@op(
    retry_policy=RetryPolicy(
        max_retries=2,
        delay=30,   # seconds
    ),
    tags={"tool": "dbt"},
)
def run_dbt_build():
    logger = get_dagster_logger()

    if not DBT_PROJECT_DIR.exists():
        raise FileNotFoundError(f"DBT project dir not found: {DBT_PROJECT_DIR}")

    if not DBT_PROFILES_DIR.exists():
        raise FileNotFoundError(f"DBT profiles dir not found: {DBT_PROFILES_DIR}")

    cmd = [
        DBT_BIN,
        "build",
        "--project-dir",
        str(DBT_PROJECT_DIR),
        "--profiles-dir",
        str(DBT_PROFILES_DIR),
    ]

    logger.info(f"Executing: {' '.join(cmd)}")

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
