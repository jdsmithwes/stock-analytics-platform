from pathlib import Path
import os
import subprocess
from dagster import op, get_dagster_logger


@op
def run_dbt_build():
    logger = get_dagster_logger()

    # Resolve repo root
    repo_root = Path(__file__).resolve().parents[2]

    # Correct dbt project path
    dbt_project_dir = repo_root / "dbt" / "dbt_stockmarketproject"

    # Correct, portable profiles dir
    dbt_profiles_dir = Path(
        os.getenv("DBT_PROFILES_DIR", Path.home() / ".dbt")
    ).expanduser()

    dbt_bin = os.getenv("DBT_BIN", "dbt")

    logger.info(f"repo_root={repo_root}")
    logger.info(f"DBT_PROJECT_DIR={dbt_project_dir}")
    logger.info(f"DBT_PROFILES_DIR={dbt_profiles_dir}")
    logger.info(f"DBT_BIN={dbt_bin}")

    if not dbt_project_dir.exists():
        raise FileNotFoundError(f"DBT project directory not found: {dbt_project_dir}")

    if not dbt_profiles_dir.exists():
        raise FileNotFoundError(f"DBT profiles directory not found: {dbt_profiles_dir}")

    commands = [
        [dbt_bin, "deps"],
        [dbt_bin, "build"],
    ]

    for cmd in commands:
        logger.info(f"Running command: {' '.join(cmd)}")

        result = subprocess.run(
            cmd + [
                "--project-dir", str(dbt_project_dir),
                "--profiles-dir", str(dbt_profiles_dir),
            ],
            cwd=str(dbt_project_dir),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )

        logger.info(result.stdout)

        if result.returncode != 0:
            raise RuntimeError(f"dbt command failed: {' '.join(cmd)}")

    logger.info("dbt deps + dbt build completed successfully")
