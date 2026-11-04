"""Ejecuta dbt contra Redshift. La contraseña se lee de Secrets Manager en cada ejecución y solo vive en el
entorno del proceso hijo: no se escribe en disco."""
import logging
import os
import re
import subprocess
from pathlib import Path

from ..aws.secrets import credentials

DBT_EXECUTABLE = os.getenv("DBT_EXECUTABLE", "/home/airflow/dbt-venv/bin/dbt")
DBT_PROJECT = Path(os.getenv("DBT_PROJECT_DIR", "/opt/airflow/src/dbt"))
LOGGER = logging.getLogger("airflow.task")
SUMMARY = re.compile(r"Done\. PASS=(\d+) WARN=(\d+) ERROR=(\d+) SKIP=(\d+) TOTAL=(\d+)")


def run_dbt(command, selector=None, batch_id=None):
    """Ejecuta un comando de dbt. Devuelve el resumen PASS/WARN/ERROR/SKIP/TOTAL; lanza error si dbt falla."""
    arguments = [DBT_EXECUTABLE, command, "--project-dir", str(DBT_PROJECT), "--profiles-dir", str(DBT_PROJECT)]
    if selector:
        arguments += ["--select", selector]
    environment = dict(os.environ, DBT_TARGET="redshift")
    if command != "deps":
        user, password = credentials("REDSHIFT_SECRET_ARN")
        environment.update(REDSHIFT_USER=user, REDSHIFT_PASSWORD=password)
    prefix = f"[lote={batch_id}] " if batch_id else ""
    LOGGER.info("%sdbt %s %s", prefix, command, selector or "")
    result = subprocess.run(arguments, capture_output=True, text=True, env=environment)
    for line in result.stdout.splitlines():
        LOGGER.info("%sdbt| %s", prefix, line)
    if result.returncode:
        raise RuntimeError(f"dbt {command} {selector or ''} falló (código {result.returncode}):\n{result.stdout[-3000:]}")
    match = SUMMARY.search(result.stdout)
    return dict(zip(("pass", "warn", "error", "skip", "total"), map(int, match.groups()))) if match else {}


if __name__ == "__main__":
    import sys

    logging.basicConfig(level=logging.INFO, format="%(message)s")
    if len(sys.argv) < 2:
        sys.exit("Uso: python -m ecommerce_dw.transformations.dbt_runner <comando> [selector]")
    try:
        print(run_dbt(sys.argv[1], sys.argv[2] if len(sys.argv) > 2 else None))
    except RuntimeError as error:
        print(f"FALLÓ: {str(error).splitlines()[0]}")
        sys.exit(1)
