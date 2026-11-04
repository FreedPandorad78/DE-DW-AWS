"""Extracción inicial e incremental por lote desde MySQL (RDS) y MongoDB (DocumentDB) a archivos locales.

Inicial: sin marca de avance, se extrae todo. Incremental: se extrae desde la marca de avance de cada entidad
menos una ventana de reproceso (datos tardíos). Un registro puede llegar en más de un lote: raw los acumula y
staging toma la última versión de cada llave.
"""
import csv
import hashlib
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .documents import flatten_document
from .schemas import BY_NAME, ENTITIES

BATCH_SIZE = 1000


def new_batch_id(now=None):
    """Identificador de lote ordenable: AAAAMMDDTHHMMSSZ (UTC)."""
    return (now or datetime.now(timezone.utc)).strftime("%Y%m%dT%H%M%SZ")


def _csv_value(value):
    if isinstance(value, datetime):
        return value.isoformat(sep=" ")
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value


def _text(value):
    return value.isoformat(sep=" " if getattr(value, "tzinfo", None) is None else "T") if isinstance(value, datetime) else str(value)


def _parse(value):
    return datetime.fromisoformat(value) if value else None


def write_csv(path, headers, rows, watermark_index=None):
    """Escribe el CSV de una entidad. Devuelve (filas, máximo de la columna de marca de avance, sha256)."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    count, highest = 0, None
    with temporary.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.writer(stream)
        writer.writerow(headers)
        for row in rows:
            writer.writerow([_csv_value(v) for v in row])
            count += 1
            if watermark_index is not None and row[watermark_index] is not None:
                value = row[watermark_index]
                highest = value if highest is None or value > highest else highest
    temporary.replace(path)
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    return count, highest, digest


def _since(entity, watermarks):
    """Inicio de la ventana de extracción (None = extracción inicial)."""
    owner = BY_NAME[(entity.source, entity.follows)] if entity.follows else entity
    value = watermarks.get((owner.source, owner.name))
    if not value:
        return None
    return _parse(value) - timedelta(days=owner.lookback_days)


def _record(batch_id, entity, path, count, since, highest, digest, previous):
    watermark_to = _text(highest) if highest is not None else previous
    return {
        "batch_id": batch_id, "source": entity.source, "entity": entity.name, "rows": count,
        "extracted_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "watermark_column": entity.watermark_column or f"(sigue a {entity.follows})",
        "watermark_from": _text(since) if since else None, "watermark_to": watermark_to,
        "sha256": digest, "local_path": str(path), "columns": entity.column_names,
    }


def _mysql_query(entity, since):
    columns = entity.column_names
    if entity.name == "order_items":
        select = ", ".join(f"i.{c}" for c in columns)
        if since is None:
            return f"SELECT {select} FROM order_items i ORDER BY i.{entity.key}", ()
        return (f"SELECT {select} FROM order_items i JOIN orders o ON o.order_id = i.order_id "
                f"WHERE o.order_date >= %s ORDER BY i.{entity.key}", (since,))
    select = ", ".join(columns)
    if since is None or entity.watermark_column is None:
        return f"SELECT {select} FROM {entity.name} ORDER BY {entity.key}", ()
    operator = ">=" if entity.inclusive else ">"
    return f"SELECT {select} FROM {entity.name} WHERE {entity.watermark_column} {operator} %s ORDER BY {entity.key}", (since,)


def extract_mysql(connection, directory, batch_id, watermarks):
    records = []
    for entity in (e for e in ENTITIES if e.source == "mysql"):
        since = _since(entity, watermarks)
        sql, params = _mysql_query(entity, since)
        cursor = connection.cursor()
        try:
            cursor.execute(sql, params)

            def rows():
                while batch := cursor.fetchmany(BATCH_SIZE):
                    yield from batch

            index = entity.column_names.index(entity.watermark_column) if entity.watermark_column else None
            path = Path(directory) / "mysql" / entity.name / "data.csv"
            count, highest, digest = write_csv(path, entity.column_names, rows(), index)
        finally:
            cursor.close()
        records.append(_record(batch_id, entity, path, count, since, highest, digest,
                               watermarks.get((entity.source, entity.name))))
    return records


def extract_mongo(client, database_name, directory, batch_id, watermarks):
    records = []
    database = client[database_name]
    for entity in (e for e in ENTITIES if e.source == "mongodb"):
        since = _since(entity, watermarks)
        if since is not None and since.tzinfo is None:
            since = since.replace(tzinfo=timezone.utc)
        query = {entity.watermark_column: {"$gte" if entity.inclusive else "$gt": since}} if since else {}
        cursor = database[entity.name].find(query).sort("_id", 1).batch_size(BATCH_SIZE)
        index = entity.column_names.index(entity.watermark_column)
        path = Path(directory) / "mongodb" / entity.name / "data.csv"
        rows = (flatten_document(entity.name, document) for document in cursor)
        count, highest, digest = write_csv(path, entity.column_names, rows, index)
        records.append(_record(batch_id, entity, path, count, since, highest, digest,
                               watermarks.get((entity.source, entity.name))))
    return records
