"""Definición única de las entidades de las fuentes y de sus tablas raw (Redshift y DuckDB de pruebas).

raw acumula los lotes: cada fila lleva _batch_id (lote de origen), _loaded_at y _source. La capa staging de dbt
toma la última versión de cada llave.
"""
from dataclasses import dataclass, field

# (nombre, tipo lógico). Los tipos con longitud se resuelven en sql_type().
MYSQL = {
    "customers": [("customer_id", "bigint"), ("name", "text"), ("email", "text"), ("phone", "text"), ("city", "text"),
                  ("department", "text"), ("registration_date", "date"), ("updated_at", "timestamp")],
    "products": [("product_id", "bigint"), ("name", "text"), ("category", "text"), ("price", "decimal(12,2)"),
                 ("description", "longtext"), ("is_active", "boolean"), ("updated_at", "timestamp")],
    "orders": [("order_id", "bigint"), ("customer_id", "bigint"), ("order_date", "timestamp"), ("status", "text"),
               ("sales_channel", "text"), ("payment_method", "text"), ("total", "decimal(14,2)")],
    "order_items": [("order_item_id", "bigint"), ("order_id", "bigint"), ("product_id", "bigint"),
                    ("quantity", "integer"), ("unit_price", "decimal(12,2)"), ("subtotal", "decimal(14,2)")],
}
MONGO = {
    "reviews": [("review_id", "bigint"), ("customer_id", "bigint"), ("product_id", "bigint"), ("rating", "integer"),
                ("reviewed_at", "timestamptz"), ("document_json", "json")],
    "navigation_events": [("event_id", "bigint"), ("event_type", "text"), ("occurred_at", "timestamptz"),
                          ("customer_id", "bigint"), ("product_id", "bigint"), ("sales_channel", "text"),
                          ("document_json", "json")],
}


@dataclass(frozen=True)
class Entity:
    source: str               # mysql | mongodb
    name: str                 # customers, orders, ...
    columns: tuple            # ((nombre, tipo), ...)
    key: str                  # llave de negocio
    watermark_column: str     # columna de negocio que avanza (None: sigue a otra entidad)
    lookback_days: int = 0    # días hacia atrás que se vuelven a extraer para absorber datos tardíos
    follows: str = None       # entidad cuya ventana de extracción comparte (order_items sigue a orders)
    inclusive: bool = True    # True: >= (con ventana de reproceso); False: > estricto (dimensiones con updated_at)

    @property
    def raw_table(self):
        prefix = "mysql" if self.source == "mysql" else "mongo"
        return f"{prefix}_{self.name}"

    @property
    def column_names(self):
        return [c for c, _ in self.columns]

    @property
    def source_label(self):
        return f"{self.source}.{self.name}"


def _entities(lookback_days=7):
    e = []
    e.append(Entity("mysql", "customers", tuple(MYSQL["customers"]), "customer_id", "updated_at", inclusive=False))
    e.append(Entity("mysql", "products", tuple(MYSQL["products"]), "product_id", "updated_at", inclusive=False))
    e.append(Entity("mysql", "orders", tuple(MYSQL["orders"]), "order_id", "order_date", lookback_days))
    e.append(Entity("mysql", "order_items", tuple(MYSQL["order_items"]), "order_item_id", None, lookback_days, "orders"))
    e.append(Entity("mongodb", "reviews", tuple(MONGO["reviews"]), "review_id", "reviewed_at", lookback_days))
    e.append(Entity("mongodb", "navigation_events", tuple(MONGO["navigation_events"]), "event_id", "occurred_at", lookback_days))
    return e


ENTITIES = _entities()
BY_NAME = {(e.source, e.name): e for e in ENTITIES}

_REDSHIFT = {"text": "varchar(1024)", "longtext": "varchar(4096)", "json": "varchar(65535)"}
_DUCKDB = {"text": "varchar", "longtext": "varchar", "json": "varchar"}


def sql_type(logical, dialect):
    table = _REDSHIFT if dialect == "redshift" else _DUCKDB
    return table.get(logical, logical)


def column_ddl(columns, dialect):
    return ", ".join(f"{name} {sql_type(kind, dialect)}" for name, kind in columns)


def raw_table_ddl(entity, dialect="redshift"):
    meta = [("_batch_id", "varchar(32)" if dialect == "redshift" else "varchar"), ("_loaded_at", "timestamptz"),
            ("_source", "varchar(64)" if dialect == "redshift" else "varchar")]
    return (f'CREATE TABLE IF NOT EXISTS "raw".{entity.raw_table} ('
            f'{column_ddl(entity.columns, dialect)}, {column_ddl(meta, dialect)})')
