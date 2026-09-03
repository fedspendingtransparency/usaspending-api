import os
import subprocess
from typing import Optional
from urllib.parse import urlparse


def build_psql_env(
    dsn: str,
    statement_timeout_hours: Optional[int] = None,
    work_mem_mb: Optional[int] = None,
    base_env: Optional[dict] = None,
) -> dict:
    """Build PostgreSQL environment variables from a database connection string."""

    if not dsn:
        raise ValueError("DSN cannot be empty")

    db_url = urlparse(dsn)

    env = (base_env or os.environ).copy()

    # Set PostgreSQL connection parameters
    env["PGHOST"] = db_url.hostname or "localhost"
    env["PGPORT"] = str(db_url.port or 5432)
    env["PGUSER"] = db_url.username or "postgres"
    env["PGPASSWORD"] = db_url.password or ""
    env["PGDATABASE"] = db_url.path.lstrip("/") if db_url.path else "postgres"

    # Set optional PostgreSQL options
    if statement_timeout_hours or work_mem_mb:
        options = []
        if statement_timeout_hours:
            options.append(f"--statement-timeout={statement_timeout_hours}h")
        if work_mem_mb:
            options.append(f"--work-mem={work_mem_mb}MB")
        env["PGOPTIONS"] = " ".join(options)

    return env


def run_psql_to_file(  # noqa: PLR0915
    sql_path: str, output_path: str, env: dict, quiet: bool = True, on_error_stop: bool = True
) -> None:
    """
    Execute a psql command that reads SQL from a file and writes output to another file.
    """
    import logging

    logger = logging.getLogger(__name__)

    # Log the SQL file contents for debugging
    try:
        with open(sql_path, "r") as f:
            sql_content = f.read()
            logger.info(f"SQL file contents (first 500 chars): {sql_content[:500]}")
    except Exception as e:
        logger.error(f"Could not read SQL file: {e}")

    psql_args = ["psql"]

    if quiet:
        psql_args.append("-q")

    psql_args.extend(["-o", output_path])

    if on_error_stop:
        psql_args.extend(["-v", "ON_ERROR_STOP=1"])

    # Test database connection first
    try:
        logger.info("Testing database connection...")
        test_process = subprocess.run(["psql", "-c", "SELECT 1;"], env=env, capture_output=True, timeout=5)
        if test_process.returncode != 0:
            logger.error(f"Database connection test failed: {test_process.stderr.decode()}")
            raise Exception(f"Cannot connect to database: {test_process.stderr.decode()}")
        logger.info("Database connection test successful")
    except subprocess.TimeoutExpired:
        logger.error("Test Connection Process timed out! Killing processes...")
        raise Exception("psql process timed out after 30 seconds") from None

    logger.info("Starting cat and psql processes...")

    # Start cat process
    cat_process = subprocess.Popen(["cat", sql_path], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    # Start psql process with cat's stdout as stdin
    psql_process = subprocess.Popen(
        psql_args,
        stdin=cat_process.stdout,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,  # Changed to PIPE to capture stderr separately
        env=env,
    )

    # Close cat's stdout in parent so psql gets EOF when cat exits
    cat_process.stdout.close()

    logger.info("Waiting for processes to complete...")

    # Wait for both processes to complete with timeout
    try:
        psql_output, psql_error = psql_process.communicate()
        cat_process.wait(timeout=30)
    except subprocess.TimeoutExpired:
        logger.error("Process timed out! Killing processes...")
        psql_process.kill()
        cat_process.kill()

        # communicate statement to finish handling data pipes
        psql_process.communicate()
        raise Exception(
            "psql process timed out by the server's process OR cat process timed out after 30 seconds"
        ) from None

    logger.info(f"psql return code: {psql_process.returncode}")
    logger.info(f"psql stdout: {psql_output.decode() if psql_output else 'empty'}")
    logger.info(f"psql stderr: {psql_error.decode() if psql_error else 'empty'}")

    # Check for errors
    if psql_process.returncode != 0:
        error_msg = psql_error.decode() if psql_error else psql_output.decode() if psql_output else "Unknown error"
        logger.error(f"psql failed: {error_msg}")
        if psql_process.poll() is None:
            psql_process.kill()
        if cat_process.poll() is None:
            cat_process.kill()
        raise subprocess.CalledProcessError(psql_process.returncode, psql_args, output=psql_output, stderr=psql_error)

    logger.info("psql completed successfully")
