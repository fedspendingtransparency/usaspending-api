import os
import subprocess
from typing import Optional
from urllib.parse import urlparse


def build_psql_env(
        dsn: str,
        statement_timeout_hours: Optional[int] = None,
        work_mem_mb: Optional[int] = None,
        base_env: Optional[dict] = None
) -> dict:
    """
    Build PostgreSQL environment variables from a database connection string.

    Args:
        dsn: Database connection string (e.g., postgresql://user:pass@host:port/dbname)
        statement_timeout_hours: Optional statement timeout in hours
        work_mem_mb: Optional work memory in MB
        base_env: Base environment to copy from (defaults to os.environ)

    Returns:
        Dictionary of environment variables for psql
    """
    db_url = urlparse(dsn)

    env = (base_env or os.environ).copy()

    # Set PostgreSQL connection parameters
    env["PGHOST"] = db_url.hostname
    env["PGPORT"] = str(db_url.port or 5432)
    env["PGUSER"] = db_url.username
    env["PGPASSWORD"] = db_url.password
    env["PGDATABASE"] = db_url.path.lstrip('/')

    # Set optional PostgreSQL options
    if statement_timeout_hours or work_mem_mb:
        options = []
        if statement_timeout_hours:
            options.append(f"--statement-timeout={statement_timeout_hours}h")
        if work_mem_mb:
            options.append(f"--work-mem={work_mem_mb}MB")
        env["PGOPTIONS"] = " ".join(options)

    return env


def run_psql_to_file(
        sql_path: str,
        output_path: str,
        env: dict,
        quiet: bool = True,
        on_error_stop: bool = True
) -> subprocess.CompletedProcess:
    """
    Execute a psql command that reads SQL from a file and writes output to another file.

    Args:
        sql_path: Path to SQL file to execute
        output_path: Path where psql should write output
        env: Environment variables (should include PGHOST, PGUSER, etc.)
        quiet: If True, suppress psql output messages
        on_error_stop: If True, stop on first error

    Returns:
        CompletedProcess object from subprocess

    Raises:
        subprocess.CalledProcessError: If psql command fails
    """
    psql_args = ["psql"]

    if quiet:
        psql_args.append("-q")

    psql_args.extend(["-o", output_path])

    if on_error_stop:
        psql_args.extend(["-v", "ON_ERROR_STOP=1"])

    # Pipe SQL file content to psql
    cat_command = subprocess.Popen(["cat", sql_path], stdout=subprocess.PIPE)

    return subprocess.check_output(
        psql_args,
        stdin=cat_command.stdout,
        stderr=subprocess.STDOUT,
        env=env,
    )
