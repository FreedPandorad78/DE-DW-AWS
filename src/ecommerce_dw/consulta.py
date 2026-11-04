"""Consulta Redshift desde la EC2 (el clúster no es público).
  python -m ecommerce_dw.consulta "select count(*) from marts.fct_sales"
  echo "select ..." | python -m ecommerce_dw.consulta
  python -m ecommerce_dw.consulta --resumen
  python -m ecommerce_dw.consulta --foto          (conteos y huellas de los hechos, en JSON)
  echo "select ..." | python -m ecommerce_dw.consulta --json
"""
import sys

from .aws import connections

RESUMEN = [
    ("Lotes registrados (control.batches)",
     "select batch_id, mode, status, to_char(started_at, 'HH24:MI:SS') as inicio, to_char(finished_at, 'HH24:MI:SS') as fin from control.batches order by batch_id"),
    ("Marcas de avance (control.watermarks)",
     "select source, entity, watermark_value, batch_id from control.watermarks order by source, entity"),
    ("Filas de raw por lote",
     """select t, _batch_id as lote, n from (
        select 'mysql_customers' t, _batch_id, count(*) n from "raw".mysql_customers group by 2
        union all select 'mysql_products', _batch_id, count(*) from "raw".mysql_products group by 2
        union all select 'mysql_orders', _batch_id, count(*) from "raw".mysql_orders group by 2
        union all select 'mysql_order_items', _batch_id, count(*) from "raw".mysql_order_items group by 2
        union all select 'mongo_reviews', _batch_id, count(*) from "raw".mongo_reviews group by 2
        union all select 'mongo_navigation_events', _batch_id, count(*) from "raw".mongo_navigation_events group by 2) x order by t, lote"""),
    ("Hechos (marts)",
     """select 'fct_sales' as hecho, count(*) as filas, count(distinct sale_sk) as llaves_distintas, cast(sum(subtotal) as varchar) as total from marts.fct_sales
        union all select 'fct_reviews', count(*), count(distinct review_sk), '-' from marts.fct_reviews
        union all select 'fct_navigation_events', count(*), count(distinct navigation_event_sk), '-' from marts.fct_navigation_events"""),
    ("Dimensiones (marts)",
     """select 'dim_customer' as dimension, count(*) as filas, sum(case when is_current then 1 else 0 end) as vigentes from marts.dim_customer
        union all select 'dim_product', count(*), sum(case when is_current then 1 else 0 end) from marts.dim_product
        union all select 'dim_date', count(*), 0 from marts.dim_date
        union all select 'dim_geography', count(*), 0 from marts.dim_geography
        union all select 'dim_sales_channel', count(*), 0 from marts.dim_sales_channel
        union all select 'dim_event_type', count(*), 0 from marts.dim_event_type"""),
    ("Cuarentena por motivo y lote de origen",
     """select t as tabla, rejection_reason as motivo, _batch_id as lote_de_origen, n from (
        select 'customers' t, rejection_reason, _batch_id, count(*) n from quarantine.quarantine_mysql__customers group by 2, 3
        union all select 'products', rejection_reason, _batch_id, count(*) from quarantine.quarantine_mysql__products group by 2, 3
        union all select 'orders', rejection_reason, _batch_id, count(*) from quarantine.quarantine_mysql__orders group by 2, 3
        union all select 'order_items', rejection_reason, _batch_id, count(*) from quarantine.quarantine_mysql__order_items group by 2, 3
        union all select 'reviews', rejection_reason, _batch_id, count(*) from quarantine.quarantine_mongodb__reviews group by 2, 3
        union all select 'navigation_events', rejection_reason, _batch_id, count(*) from quarantine.quarantine_mongodb__navigation_events group by 2, 3) x
        order by t, motivo, lote_de_origen"""),
]


def show(cursor, sql):
    cursor.execute(sql)
    if not cursor.description:
        print("OK")
        return
    columns = [d[0] for d in cursor.description]
    rows = [["" if v is None else str(v) for v in row] for row in cursor.fetchall()]
    widths = [max(len(c), *(len(r[i]) for r in rows)) if rows else len(c) for i, c in enumerate(columns)]
    line = " | ".join(f"{{:<{w}}}" for w in widths)
    print(line.format(*columns))
    print("-+-".join("-" * w for w in widths))
    for row in rows:
        print(line.format(*row))
    print(f"({len(rows)} filas)")


def _fact(table, key, extra):
    return (f"select count(*), count(distinct {key}), cast(coalesce(sum({extra}), 0) as varchar), "
            f"coalesce(sum(strtol(substring(md5({key} || '|' || coalesce(customer_sk, '') || '|' || coalesce(product_sk, '')), 1, 8), 16)), 0) "
            f"from marts.{table}")


FOTO = {
    "ventas": _fact("fct_sales", "sale_sk", "subtotal"),
    "resenas": _fact("fct_reviews", "review_sk", "rating"),
    "eventos": _fact("fct_navigation_events", "navigation_event_sk", "0"),
    "dim_customer": "select count(*), sum(case when is_current then 1 else 0 end) from marts.dim_customer",
    "dim_product": "select count(*), sum(case when is_current then 1 else 0 end) from marts.dim_product",
    "cuarentena": """select count(*) from (
        select 1 from quarantine.quarantine_mysql__customers union all select 1 from quarantine.quarantine_mysql__products
        union all select 1 from quarantine.quarantine_mysql__orders union all select 1 from quarantine.quarantine_mysql__order_items
        union all select 1 from quarantine.quarantine_mongodb__reviews union all select 1 from quarantine.quarantine_mongodb__navigation_events) q""",
    "lotes_completos": "select count(*) from control.batches where status = 'COMPLETO'",
}


def rows_of(cursor, sql):
    cursor.execute(sql)
    columns = [d[0] for d in cursor.description] if cursor.description else []
    return columns, [[None if v is None else (v if isinstance(v, (int, float, str, bool)) else str(v)) for v in row] for row in cursor.fetchall()]


def main():
    conn = connections.connect_redshift()
    cursor = conn.cursor()
    try:
        if sys.argv[1:] == ["--foto"]:
            import json
            print(json.dumps({name: rows_of(cursor, sql)[1][0] for name, sql in FOTO.items()}))
        elif sys.argv[1:2] == ["--json"]:
            import json
            sql = " ".join(sys.argv[2:]).strip() or sys.stdin.read().strip()
            columns, rows = rows_of(cursor, sql)
            print(json.dumps({"columns": columns, "rows": rows}))
        elif sys.argv[1:] == ["--resumen"]:
            for title, sql in RESUMEN:
                print(f"\n== {title}")
                show(cursor, sql)
        else:
            sql = " ".join(sys.argv[1:]).strip() or sys.stdin.read().strip()
            if not sql:
                sys.exit(__doc__)
            show(cursor, sql)
    finally:
        conn.rollback()
        conn.close()


if __name__ == "__main__":
    main()
