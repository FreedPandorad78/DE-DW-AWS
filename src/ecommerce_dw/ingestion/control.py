"""Tablas de control: registro de lotes, de sus entidades y marcas de avance.

Un lote queda identificado por batch_id. Su estado avanza: EXTRAIDO -> EN_S3 -> CARGADO -> TRANSFORMADO -> COMPLETO
(o FALLIDO). La MARCA DE AVANCE de cada entidad (control.watermarks) solo se mueve cuando el lote termina COMPLETO,
es decir, después de las pruebas de calidad: si algo falla antes, el siguiente lote vuelve a pedir la misma ventana.
Las consultas usan el estilo de parámetros %s (redshift_connector).
"""
from datetime import datetime, timezone

DDL = [
    "CREATE SCHEMA IF NOT EXISTS control",
    """CREATE TABLE IF NOT EXISTS control.batches (
        batch_id varchar(32), mode varchar(32), airflow_run_id varchar(256), status varchar(32),
        started_at timestamptz, finished_at timestamptz, detail varchar(2048))""",
    """CREATE TABLE IF NOT EXISTS control.batch_entities (
        batch_id varchar(32), source varchar(32), entity varchar(64), s3_prefix varchar(512),
        rows_extracted bigint, rows_loaded bigint, extracted_at timestamptz, watermark_from varchar(64),
        watermark_to varchar(64), sha256 varchar(64), status varchar(32), loaded_at timestamptz)""",
    """CREATE TABLE IF NOT EXISTS control.watermarks (
        source varchar(32), entity varchar(64), watermark_value varchar(64), batch_id varchar(32),
        updated_at timestamptz)""",
]


def _now():
    return datetime.now(timezone.utc)


def _as_datetime(value):
    parsed = datetime.fromisoformat(value)
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def ensure_tables(conn):
    cursor = conn.cursor()
    for statement in DDL:
        cursor.execute(statement)
    conn.commit()


def start_batch(conn, batch_id, mode, airflow_run_id):
    cursor = conn.cursor()
    cursor.execute("SELECT count(*) FROM control.batches WHERE batch_id = %s", (batch_id,))
    if cursor.fetchone()[0]:
        cursor.execute("UPDATE control.batches SET status = %s, airflow_run_id = %s, finished_at = NULL, detail = NULL "
                       "WHERE batch_id = %s", ("INICIADO", airflow_run_id, batch_id))
    else:
        cursor.execute("INSERT INTO control.batches VALUES (%s, %s, %s, %s, %s, NULL, NULL)",
                       (batch_id, mode, airflow_run_id, "INICIADO", _now()))
    conn.commit()


def set_batch_status(conn, batch_id, status, detail=None):
    finished = _now() if status in ("COMPLETO", "FALLIDO") else None
    cursor = conn.cursor()
    cursor.execute("UPDATE control.batches SET status = %s, finished_at = %s, detail = %s WHERE batch_id = %s",
                   (status, finished, (detail or "")[:2000] or None, batch_id))
    conn.commit()


def register_entities(conn, batch_id, manifests, status="EN_S3"):
    cursor = conn.cursor()
    for m in manifests:
        cursor.execute("DELETE FROM control.batch_entities WHERE batch_id = %s AND source = %s AND entity = %s",
                       (batch_id, m["source"], m["entity"]))
        cursor.execute(
            "INSERT INTO control.batch_entities VALUES (%s, %s, %s, %s, %s, NULL, %s, %s, %s, %s, %s, NULL)",
            (batch_id, m["source"], m["entity"], m["s3_prefix"], m["rows"], m["extracted_at"],
             m["watermark_from"], m["watermark_to"], m["sha256"], status))
    conn.commit()


def mark_loaded(conn, batch_id, source, entity, rows_loaded):
    cursor = conn.cursor()
    cursor.execute("UPDATE control.batch_entities SET rows_loaded = %s, loaded_at = %s, status = %s "
                   "WHERE batch_id = %s AND source = %s AND entity = %s",
                   (rows_loaded, _now(), "CARGADO", batch_id, source, entity))
    conn.commit()


def get_watermarks(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT source, entity, watermark_value FROM control.watermarks")
    return {(source, entity): value for source, entity, value in cursor.fetchall()}


def advance_watermarks(conn, batch_id, manifests):
    """Mueve la marca de avance de cada entidad al máximo extraído en el lote. Solo se llama con el lote COMPLETO."""
    cursor = conn.cursor()
    current = get_watermarks(conn)
    for m in manifests:
        if m["watermark_to"] is None or m["watermark_column"].startswith("(sigue"):
            continue
        previous = current.get((m["source"], m["entity"]))
        if previous and _as_datetime(previous) >= _as_datetime(m["watermark_to"]):
            continue  # solo hacia adelante: reprocesar un lote antiguo no retrocede la marca
        cursor.execute("DELETE FROM control.watermarks WHERE source = %s AND entity = %s", (m["source"], m["entity"]))
        cursor.execute("INSERT INTO control.watermarks VALUES (%s, %s, %s, %s, %s)",
                       (m["source"], m["entity"], m["watermark_to"], batch_id, _now()))
    conn.commit()
