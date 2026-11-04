"""Carga de un lote desde S3 a las tablas raw de Redshift con COPY.

Cada entidad se carga en UNA transacción: tabla temporal -> COPY -> borrar el lote en raw -> insertar -> verificar
conteo. Repetir el lote reemplaza sus filas (no las duplica) y un fallo no deja filas a medias.
"""
import logging

from . import control
from .schemas import BY_NAME, column_ddl, raw_table_ddl

LOGGER = logging.getLogger(__name__)


class BatchLoader:
    dialect = "redshift"

    def __init__(self, conn, bucket, iam_role_arn, region=None):
        self.conn, self.bucket, self.iam_role_arn, self.region = conn, bucket, iam_role_arn, region

    def ensure_raw_tables(self):
        cursor = self.conn.cursor()
        cursor.execute('CREATE SCHEMA IF NOT EXISTS "raw"')
        for entity in BY_NAME.values():
            cursor.execute(raw_table_ddl(entity, self.dialect))
        self.conn.commit()

    def copy_sql(self, stage, entity, manifest):
        columns = ", ".join(entity.column_names)
        region = f" REGION '{self.region}'" if self.region else ""
        return (f"COPY {stage} ({columns}) FROM 's3://{manifest['s3_bucket']}/{manifest['s3_key']}' "
                f"IAM_ROLE '{self.iam_role_arn}' FORMAT AS CSV IGNOREHEADER 1 "
                f"TIMEFORMAT 'auto' DATEFORMAT 'auto' EMPTYASNULL{region}")

    def copy_into_stage(self, cursor, stage, entity, manifest):
        cursor.execute(self.copy_sql(stage, entity, manifest))

    def load_entity(self, batch_id, manifest):
        entity = BY_NAME[(manifest["source"], manifest["entity"])]
        stage = f"stage_{entity.raw_table}"
        cursor = self.conn.cursor()
        try:
            cursor.execute(f"DROP TABLE IF EXISTS {stage}")
            cursor.execute(f"CREATE TEMP TABLE {stage} ({column_ddl(entity.columns, self.dialect)})")
            self.copy_into_stage(cursor, stage, entity, manifest)
            cursor.execute(f"SELECT count(*) FROM {stage}")
            staged = cursor.fetchone()[0]
            if staged != manifest["rows"]:
                raise ValueError(f"{entity.raw_table}: el manifiesto dice {manifest['rows']} filas y COPY cargó {staged}")
            cursor.execute(f'DELETE FROM "raw".{entity.raw_table} WHERE _batch_id = %s', (batch_id,))
            cursor.execute(f'INSERT INTO "raw".{entity.raw_table} SELECT {", ".join(entity.column_names)}, %s, current_timestamp, %s '
                           f"FROM {stage}", (batch_id, entity.source_label))
            cursor.execute(f'SELECT count(*) FROM "raw".{entity.raw_table} WHERE _batch_id = %s', (batch_id,))
            loaded = cursor.fetchone()[0]
            if loaded != manifest["rows"]:
                raise ValueError(f"{entity.raw_table}: raw quedó con {loaded} filas del lote y se esperaban {manifest['rows']}")
            cursor.execute(f"DROP TABLE IF EXISTS {stage}")
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise
        control.mark_loaded(self.conn, batch_id, entity.source, entity.name, loaded)
        LOGGER.info("lote=%s %s: %s filas cargadas en raw.%s", batch_id, entity.source_label, loaded, entity.raw_table)
        return loaded

    def load_batch(self, batch_id, batch_manifest, fail_after=None):
        """fail_after: solo para demostraciones. Falla después de cargar esa cantidad de entidades."""
        self.ensure_raw_tables()
        loaded = {}
        for m in batch_manifest["entities"]:
            if fail_after is not None and len(loaded) == fail_after:
                raise RuntimeError(f"FALLO SIMULADO tras cargar {len(loaded)} entidades: las cargadas ya están en raw; "
                                   "la reejecución del lote reemplaza sus filas sin duplicar")
            loaded[f"{m['source']}.{m['entity']}"] = self.load_entity(batch_id, m)
        return loaded
