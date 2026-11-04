"""Lógica de cada paso del DAG dw_pipeline. Está separada del DAG para poder probarla sin Airflow."""
import json
import os
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

from .aws import connections
from .ingestion import control, extract, s3_batches
from .ingestion.load_redshift import BatchLoader
from .logging_utils import BatchLog
from .transformations.dbt_runner import run_dbt

REQUIRED_ENV = ["S3_BUCKET", "REDSHIFT_HOST", "REDSHIFT_SECRET_ARN", "REDSHIFT_IAM_ROLE_ARN",
                "RDS_HOST", "RDS_SECRET_ARN", "DOCDB_HOST", "DOCDB_SECRET_ARN", "DOCDB_CA_BUNDLE"]
FAIL_AFTER_ENTITIES = 2


def bucket():
    return os.environ["S3_BUCKET"]


def s3_client():
    return boto3.client("s3", region_name=os.getenv("AWS_REGION", "us-east-2"))


def extract_dir(batch_id):
    return Path(os.getenv("EXTRACT_DIR", "/opt/airflow/extracts")) / batch_id


def put_json(key, data):
    s3_client().put_object(Bucket=bucket(), Key=key, ContentType="application/json",
                           Body=json.dumps(data, ensure_ascii=False, indent=2, default=str).encode())


def should_fail_once(params, stage, batch_id):
    """Fallo controlado para las demostraciones: True UNA sola vez por lote y etapa (el reintento se recupera)."""
    if (params or {}).get("simular_fallo") != stage:
        return False
    key = f"_state/fallos_simulados/batch_id={batch_id}/{stage}"
    try:
        s3_client().head_object(Bucket=bucket(), Key=key)
        return False
    except ClientError as error:
        if error.response["Error"]["Code"] not in ("404", "NoSuchKey", "NotFound"):
            raise
    s3_client().put_object(Bucket=bucket(), Key=key, Body=b"fallo simulado ya ejecutado")
    return True


# ---------------------------------------------------------------- 1. validar conexiones y parámetros
def validate(params, dag_run):
    reprocess = ((params or {}).get("reprocess_batch_id") or "").strip()
    # dag_run.start_date no cambia entre reintentos: el lote es estable dentro de una misma ejecución
    batch_id = reprocess or extract.new_batch_id(dag_run.start_date)
    log = BatchLog("1_validar_conexiones_y_parametros", batch_id)
    missing = [name for name in REQUIRED_ENV if not os.getenv(name)]
    if missing:
        raise ValueError(f"Faltan variables de entorno: {missing}")
    if reprocess:
        s3_batches.read_batch(s3_client(), bucket(), reprocess)  # lanza error si el lote no existe en S3
        log.info("modo REPROCESO: se reutilizan los archivos del lote en S3; no se consultan las fuentes")
    mysql = connections.connect_mysql()
    mysql.cursor().execute("SELECT 1")
    mysql.close()
    mongo = connections.connect_mongo()
    mongo.admin.command("ping")
    mongo.close()
    s3_client().head_bucket(Bucket=bucket())
    conn = connections.connect_redshift()
    conn.cursor().execute("SELECT 1")
    control.ensure_tables(conn)
    mode = "REPROCESO" if reprocess else "NUEVA_EXTRACCION"
    control.start_batch(conn, batch_id, mode, dag_run.run_id)
    conn.close()
    log.info(f"acceso verificado: RDS MySQL, DocumentDB, S3 y Redshift; lote {batch_id} ({mode})")
    return {"batch_id": batch_id, "mode": mode, "airflow_run_id": dag_run.run_id}


def _watermarks():
    conn = connections.connect_redshift()
    try:
        return control.get_watermarks(conn)
    finally:
        conn.close()


# ---------------------------------------------------------------- 2. extraer
def extract_mysql_step(info):
    log = BatchLog("2_extraer_mysql", info["batch_id"])
    if info["mode"] == "REPROCESO":
        log.info("reproceso: no se consulta MySQL")
        return []
    connection = connections.connect_mysql()
    try:
        records = extract.extract_mysql(connection, extract_dir(info["batch_id"]), info["batch_id"], _watermarks())
    finally:
        connection.close()
    for r in records:
        log.info(f"{r['source']}.{r['entity']}: {r['rows']} filas (desde {r['watermark_from'] or 'el inicio'})")
    return records


def extract_mongo_step(info):
    log = BatchLog("2_extraer_mongodb", info["batch_id"])
    if info["mode"] == "REPROCESO":
        log.info("reproceso: no se consulta DocumentDB")
        return []
    client = connections.connect_mongo(tz_aware=True)
    try:
        records = extract.extract_mongo(client, os.getenv("MONGO_DATABASE", "ecommerce"), extract_dir(info["batch_id"]),
                                        info["batch_id"], _watermarks())
    finally:
        client.close()
    for r in records:
        log.info(f"{r['source']}.{r['entity']}: {r['rows']} filas (desde {r['watermark_from'] or 'el inicio'})")
    return records


# ---------------------------------------------------------------- 3. escribir en S3
def write_s3_step(info, mysql_records, mongo_records):
    log = BatchLog("3_escribir_s3", info["batch_id"])
    if info["mode"] == "REPROCESO":
        batch = s3_batches.read_batch(s3_client(), bucket(), info["batch_id"])
        log.info(f"reproceso: {len(batch['entities'])} entidades ya en S3")
    else:
        batch = s3_batches.upload_batch(s3_client(), bucket(), info["batch_id"], list(mysql_records) + list(mongo_records))
        log.info(f"{batch['rows_total']} filas en s3://{bucket()}/raw/.../batch_id={info['batch_id']}/")
    conn = connections.connect_redshift()
    try:
        control.register_entities(conn, info["batch_id"], batch["entities"])
        control.set_batch_status(conn, info["batch_id"], "EN_S3")
    finally:
        conn.close()
    return batch


# ---------------------------------------------------------------- 4. cargar en Redshift
def load_step(info, batch, params=None):
    log = BatchLog("4_cargar_redshift", info["batch_id"])
    fail_after = FAIL_AFTER_ENTITIES if should_fail_once(params, "carga_redshift", info["batch_id"]) else None
    conn = connections.connect_redshift()
    try:
        loader = BatchLoader(conn, bucket(), os.environ["REDSHIFT_IAM_ROLE_ARN"], os.getenv("AWS_REGION", "us-east-2"))
        loaded = loader.load_batch(info["batch_id"], batch, fail_after=fail_after)
        control.set_batch_status(conn, info["batch_id"], "CARGADO")
    finally:
        conn.close()
    log.info(f"filas cargadas en raw: {loaded}")
    return loaded


# ---------------------------------------------------------------- 5. transformar con dbt
def dbt_step(info, command, selector=None, params=None, fail_stage=None):
    log = BatchLog(f"5_dbt_{command}", info["batch_id"])
    if fail_stage and should_fail_once(params, fail_stage, info["batch_id"]):
        raise RuntimeError(f"FALLO SIMULADO antes de dbt {command} {selector}: raw ya está cargado; el reintento continúa desde aquí")
    summary = run_dbt(command, selector, info["batch_id"])
    log.info(f"resultado: {summary}")
    return summary


def mark_transformed(info):
    conn = connections.connect_redshift()
    try:
        control.set_batch_status(conn, info["batch_id"], "TRANSFORMADO")
    finally:
        conn.close()


# ---------------------------------------------------------------- 6. pruebas de calidad y conciliación
SALES_BY_DATE_MISMATCH = """
select count(*) from (
    select coalesce(s.d, f.d) as d
    from (select cast(o.order_date as date) as d, sum(i.subtotal) as amount
          from intermediate.int_mysql__order_items__valid i
          join intermediate.int_mysql__orders__valid o on o.order_id = i.order_id group by 1) s
    full outer join (select dt.date as d, sum(f.subtotal) as amount
                     from marts.fct_sales f join marts.dim_date dt on dt.date_sk = f.date_sk group by 1) f on s.d = f.d
    where s.amount is null or f.amount is null or s.amount <> f.amount
) mismatches
"""
RECONCILIATION = [
    ("ventas", "select count(*), coalesce(sum(subtotal), 0) from intermediate.int_mysql__order_items__valid",
     "select count(*), coalesce(sum(subtotal), 0) from marts.fct_sales", "sale_sk", "fct_sales"),
    ("reseñas", "select count(*), 0 from intermediate.int_mongodb__reviews__valid",
     "select count(*), 0 from marts.fct_reviews", "review_sk", "fct_reviews"),
    ("eventos de navegación", "select count(*), 0 from intermediate.int_mongodb__navigation_events__valid",
     "select count(*), 0 from marts.fct_navigation_events", "navigation_event_sk", "fct_navigation_events"),
]


def reconcile(conn):
    """Concilia cada hecho con su capa validada: filas, total, llaves únicas, y el subtotal por fecha de negocio."""
    cursor = conn.cursor()
    results = []
    for name, source_sql, fact_sql, key, table in RECONCILIATION:
        source = cursor.execute(source_sql).fetchone()
        fact = cursor.execute(fact_sql).fetchone()
        duplicates = cursor.execute(f"select count(*) from (select {key} from marts.{table} group by {key} having count(*) > 1) d").fetchone()[0]
        ok = (source[0] == fact[0] and source[1] == fact[1] and duplicates == 0)
        results.append({"indicador": name, "filas_origen_valido": int(source[0]), "filas_modelo": int(fact[0]),
                        "total_origen_valido": str(source[1]), "total_modelo": str(fact[1]),
                        "llaves_duplicadas": int(duplicates), "ok": bool(ok), "sql_origen": source_sql.strip(), "sql_modelo": fact_sql.strip()})
    by_date = cursor.execute(SALES_BY_DATE_MISMATCH).fetchone()[0]
    results.append({"indicador": "subtotal de ventas por fecha de negocio", "fechas_con_diferencia": int(by_date), "ok": by_date == 0,
                    "sql": " ".join(SALES_BY_DATE_MISMATCH.split())})
    return results


def quality_step(info):
    log = BatchLog("6_pruebas_calidad", info["batch_id"])
    tests = run_dbt("test", batch_id=info["batch_id"])
    conn = connections.connect_redshift()
    try:
        results = reconcile(conn)
    finally:
        conn.close()
    report = {"batch_id": info["batch_id"], "dbt_test": tests, "conciliacion": results,
              "ejecutado_utc": datetime.now(timezone.utc).isoformat(timespec="seconds")}
    put_json(f"quality/batch_id={info['batch_id']}/reconciliation.json", report)
    failed = [r["indicador"] for r in results if not r["ok"]]
    for r in results:
        log.info(f"{'OK ' if r['ok'] else 'FALLA'} conciliación: {r['indicador']}")
    if failed:
        raise AssertionError(f"La conciliación falló para: {failed}")
    return {"dbt_test": tests, "conciliacion_ok": True}


# ---------------------------------------------------------------- 7. registrar el resultado
def record_result(info, task_states, results):
    """Siempre corre. Con éxito: el lote queda COMPLETO y avanzan las marcas de avance. Si algo falló: FALLIDO."""
    info = info or {}
    batch_id = info.get("batch_id") or "desconocido"
    failed = {t: s for t, s in task_states.items() if s != "success"}
    status = {"batch_id": batch_id, "mode": info.get("mode"), "airflow_run_id": info.get("airflow_run_id"),
              "estado": "FALLIDO" if failed else "EXITOSO", "tareas": task_states, "resultados": results,
              "finalizado_utc": datetime.now(timezone.utc).isoformat(timespec="seconds")}
    if info.get("batch_id"):
        conn = connections.connect_redshift()
        try:
            if failed:
                control.set_batch_status(conn, batch_id, "FALLIDO", f"tareas sin éxito: {failed}")
            else:
                manifest = s3_batches.read_batch(s3_client(), bucket(), batch_id)
                control.advance_watermarks(conn, batch_id, manifest["entities"])
                control.set_batch_status(conn, batch_id, "COMPLETO")
        finally:
            conn.close()
    put_json(f"runs/dag_run={_safe(info.get('airflow_run_id') or batch_id)}/pipeline_status.json", status)
    if failed:
        raise RuntimeError(f"Procesamiento FALLIDO; tareas sin éxito: {failed}")
    return status


def _safe(text):
    return "".join(c if c.isalnum() or c in "_.=-" else "_" for c in text)
