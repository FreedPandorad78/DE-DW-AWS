"""Prueba local de punta a punta del pipeline de AWS, sin AWS.

MySQL (SQLite), DocumentDB (mongomock), S3 (moto) y Redshift (DuckDB con la misma SQL de carga) simulados.
Se ejecutan los modelos de dbt de verdad. Recorre: carga inicial, lote incremental, repetición, cambio SCD2,
recuperación tras un fallo y protección contra sobrescritura en S3. Compara con la línea base del proyecto local.

  pip install dbt-core==1.9.4 dbt-duckdb==1.9.4 mongomock "moto[s3]" Faker PyMySQL pymongo boto3
  python tests/test_pipeline_local.py
"""
import os, re, shutil, sqlite3, subprocess, sys, tempfile
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
os.environ.update(AWS_ACCESS_KEY_ID="x", AWS_SECRET_ACCESS_KEY="x", AWS_DEFAULT_REGION="us-east-2")

import boto3, duckdb, mongomock
from moto import mock_aws

from ecommerce_dw import pipeline
from ecommerce_dw.ingestion import control, extract, s3_batches
from ecommerce_dw.ingestion.load_redshift import BatchLoader
from ecommerce_dw.ingestion.schemas import BY_NAME, ENTITIES, column_ddl, sql_type
from ecommerce_dw.sources import append_batch as ab, generate_data as gd

BUCKET = "bucket-prueba-dw"
WORK = Path(tempfile.mkdtemp(prefix="dw_test_"))
DB = str(WORK / "warehouse.duckdb")
sqlite3.register_adapter(datetime, lambda d: d.isoformat(sep=" "))
sqlite3.register_adapter(Decimal, str)
sqlite3.register_adapter(date, lambda d: d.isoformat())
sqlite3.register_converter("DECIMAL", lambda b: Decimal(b.decode()))


def ok(cond, msg):
    print(("  OK     " if cond else "  FALLA  ") + msg)
    if not cond:
        sys.exit(1)


# ---------------------------------------------------------------- MySQL simulado (SQLite con estilo de parámetros de PyMySQL)
class Cursor:
    def __init__(self, raw): self.raw = raw
    def _sql(self, sql): return re.sub(r"%s", "?", re.sub(r"%\((\w+)\)s", r":\1", sql))
    def execute(self, sql, params=()):
        self.raw.execute(self._sql(sql), params); return self
    def executemany(self, sql, seq): self.raw.executemany(self._sql(sql), seq)
    def fetchone(self): return self.raw.fetchone()
    def fetchall(self): return self.raw.fetchall()
    def fetchmany(self, n): return self.raw.fetchmany(n)
    def close(self): pass
    def __enter__(self): return self
    def __exit__(self, *a): return False


class FakeMySQL:
    def __init__(self):
        self.db = sqlite3.connect(":memory:", detect_types=sqlite3.PARSE_DECLTYPES)
        self.db.executescript("""
            create table customers (customer_id integer primary key, name text, email text, phone text, city text, department text, registration_date text, updated_at text);
            create table products (product_id integer primary key, name text, category text, price decimal, description text, is_active integer, updated_at text);
            create table orders (order_id integer primary key, customer_id integer, order_date text, status text, sales_channel text, payment_method text, total decimal);
            create table order_items (order_item_id integer primary key, order_id integer, product_id integer, quantity integer, unit_price decimal, subtotal decimal);""")
    def cursor(self): return Cursor(self.db.cursor())
    def commit(self): self.db.commit()


# ---------------------------------------------------------------- Redshift simulado con DuckDB
class DuckCursor:
    def __init__(self, con): self.con, self.rel = con, None
    def execute(self, sql, params=()):
        self.rel = self.con.execute(sql.replace("%s", "?"), list(params)); return self
    def fetchone(self): return self.rel.fetchone()
    def fetchall(self): return self.rel.fetchall()


class DuckConn:
    def __init__(self, path): self.con = duckdb.connect(path)
    def cursor(self): return DuckCursor(self.con)
    def commit(self): pass
    def rollback(self): pass
    def close(self): self.con.close()


class DuckLoader(BatchLoader):
    """Mismo flujo que Redshift; solo COPY cambia: descarga el CSV de S3 y lo lee con DuckDB."""
    dialect = "duckdb"
    fail_on = None
    def copy_into_stage(self, cursor, stage, entity, manifest):
        if self.fail_on == entity.raw_table:
            raise RuntimeError(f"fallo simulado de COPY en {entity.raw_table}")
        local = WORK / "descarga" / manifest["s3_key"].replace("/", "_")
        local.parent.mkdir(exist_ok=True)
        boto3.client("s3").download_file(manifest["s3_bucket"], manifest["s3_key"], str(local))
        types = "{" + ", ".join(f"'{n}': '{sql_type(t, 'duckdb')}'" for n, t in entity.columns) + "}"
        cursor.execute(f"INSERT INTO {stage} SELECT * FROM read_csv('{local}', columns={types}, header=true, nullstr='')")


def warehouse():
    return DuckConn(DB)


# ---------------------------------------------------------------- dbt
DBT_DIR = WORK / "dbt"
shutil.copytree(ROOT / "src" / "dbt", DBT_DIR, ignore=shutil.ignore_patterns("dbt_packages", "target", "logs", "package-lock.yml"))
(DBT_DIR / "packages.yml").write_text('packages:\n  - git: "https://github.com/dbt-labs/dbt-utils.git"\n    revision: 1.3.0\n')
ENV = dict(os.environ, DBT_TARGET="duckdb", DUCKDB_PATH=DB)


def dbt(*args):
    threads = [] if args[0] == "deps" else ["--threads", "1"]
    r = subprocess.run(["dbt", *args, "--project-dir", str(DBT_DIR), "--profiles-dir", str(DBT_DIR), *threads],
                       capture_output=True, text=True, env=ENV)
    if r.returncode:
        print(r.stdout[-4000:]); print(r.stderr[-1500:])
    return r.returncode == 0, r.stdout


def dbt_pipeline(full=True):
    steps = ([["deps"]] if full else []) + [["seed"], ["run", "--select", "path:models/intermediate"], ["run", "--select", "path:models/quarantine"],
                                           ["snapshot"], ["run", "--select", "path:models/marts"]]
    for s in steps:
        passed, _ = dbt(*s)
        ok(passed, "dbt " + " ".join(s))
    passed, out = dbt("test")
    summary = [l for l in out.splitlines() if "Done. PASS" in l]
    ok(passed, "dbt test: " + (summary[-1].split("Done. ")[-1] if summary else "sin resumen"))


def q(sql):
    con = duckdb.connect(DB, read_only=True)
    try: return con.execute(sql).fetchall()
    finally: con.close()


def scalar(sql): return q(sql)[0][0]


def sc(conn, sql): return conn.cursor().execute(sql).fetchone()[0]


# ---------------------------------------------------------------- un lote completo: extraer -> S3 -> raw
def run_batch(mysql, mongo, s3, batch_id, mode="NUEVA_EXTRACCION", advance=True):
    conn = warehouse()
    try:
        control.ensure_tables(conn)
        control.start_batch(conn, batch_id, mode, f"airflow-{batch_id}")
        marks = control.get_watermarks(conn)
        d = WORK / "extracts" / batch_id
        records = extract.extract_mysql(mysql, d, batch_id, marks) + extract.extract_mongo(mongo, "ecommerce", d, batch_id, marks)
        manifest = s3_batches.upload_batch(s3, BUCKET, batch_id, records)
        control.register_entities(conn, batch_id, manifest["entities"])
        control.set_batch_status(conn, batch_id, "EN_S3")
        loaded = DuckLoader(conn, BUCKET, "arn:aws:iam::CUENTA:role/prueba").load_batch(batch_id, manifest)
        control.set_batch_status(conn, batch_id, "CARGADO")
    finally:
        conn.close()
    return manifest, loaded


def finish_batch(batch_id, manifest):
    conn = warehouse()
    try:
        control.set_batch_status(conn, batch_id, "COMPLETO")
        control.advance_watermarks(conn, batch_id, manifest["entities"])
    finally:
        conn.close()


def facts():
    out = {}
    for t, key in (("fct_sales", "sale_sk"), ("fct_reviews", "review_sk"), ("fct_navigation_events", "navigation_event_sk")):
        rows, uniq = q(f"select count(*), count(distinct {key}) from marts.{t}")[0]
        digest = scalar(f"select md5(string_agg(cast({key} as varchar) || '|' || cast(customer_sk as varchar) || '|' || cast(product_sk as varchar), ',' order by {key})) from marts.{t}")
        out[t] = (rows, uniq, digest)
    return out


@mock_aws
def main():
    s3 = boto3.client("s3", region_name="us-east-2")
    s3.create_bucket(Bucket=BUCKET, CreateBucketConfiguration={"LocationConstraint": "us-east-2"})

    print("\n== Fuentes simuladas con los datos de tus scripts (semilla 42, 2 defectos por regla) ==")
    args = SimpleNamespace(customer_count=50, product_count=25, order_count=100, review_count=150, event_count=500, seed=42, bad_records=2)
    rng, fake, customers, products, orders, details, start = gd.build_rows(args)
    bad = gd.build_bad_records(args, start)
    reviews, events = gd.build_mongo_documents(args, rng, fake, customers, products, start)
    mysql = FakeMySQL()
    cur = mysql.cursor()
    gd._insert_mysql_rows(cur, customers, products, orders, details)
    gd._insert_mysql_rows(cur, bad["mysql_customers"], bad["mysql_products"], bad["mysql_orders"], bad["mysql_order_items"])
    mysql.commit()
    mongo = mongomock.MongoClient(tz_aware=True)
    mongo["ecommerce"]["reviews"].insert_many(reviews + bad["mongo_reviews"])
    mongo["ecommerce"]["navigation_events"].insert_many(events + bad["mongo_events"])

    print("\n== Lote 1: carga inicial ==")
    b1 = "20260101T100000Z"
    m1, loaded = run_batch(mysql, mongo, s3, b1)
    ok({k: v for k, v in loaded.items()} == {"mysql.customers": 54, "mysql.products": 27, "mysql.orders": 106, "mysql.order_items": 259,
                                              "mongodb.reviews": 158, "mongodb.navigation_events": 508}, f"raw cargado: {loaded}")
    ok(all(e["watermark_from"] is None for e in m1["entities"]), "carga inicial: sin marca de avance previa, se extrae todo")
    ok(scalar("select count(*) from control.watermarks") == 0, "la marca de avance NO se mueve antes de terminar el lote")
    keys = {o["Key"] for o in s3.list_objects_v2(Bucket=BUCKET, Prefix="raw/")["Contents"]}
    ok(f"raw/mysql/customers/batch_id={b1}/data.csv" in keys and f"raw/mongodb/reviews/batch_id={b1}/manifest.json" in keys, "ruta raw/<fuente>/<entidad>/batch_id=<id>/ en S3")
    dbt_pipeline(full=True)
    base = {"fct_sales": 253, "fct_reviews": 150, "fct_navigation_events": 500}
    f1 = facts()
    ok({t: f1[t][0] for t in base} == base, f"hechos = línea base del proyecto local: {base}")
    ok(q("select count(*) from marts.dim_customer")[0][0] == 50 and scalar("select count(*) from marts.dim_product") == 25, "dim_customer 50 y dim_product 25")
    ok(scalar("select round(sum(subtotal), 2) from marts.fct_sales") == Decimal("7293790.26"), "total de ventas = 7.293.790,26 (igual que el local)")
    quarantine = {t: scalar(f"select count(*) from quarantine.{t}") for t in ("quarantine_mysql__customers", "quarantine_mysql__products", "quarantine_mysql__orders",
                                                                             "quarantine_mysql__order_items", "quarantine_mongodb__reviews", "quarantine_mongodb__navigation_events")}
    ok(list(quarantine.values()) == [4, 2, 4, 6, 8, 8], f"cuarentena igual al local: {list(quarantine.values())}")
    ok(scalar("select count(*) from quarantine.quarantine_mysql__orders where _batch_id = '" + b1 + "'") == 4, "la cuarentena conserva el lote de origen (_batch_id)")
    finish_batch(b1, m1)
    ok(scalar("select count(*) from control.watermarks") == 5, "tras terminar el lote COMPLETO, avanzan las 5 marcas de avance (order_items sigue a orders)")
    ok(scalar("select status from control.batches where batch_id = '" + b1 + "'") == "COMPLETO", "lote 1 en estado COMPLETO")

    print("\n== Lote 2: incremental (3 pedidos, 3 reseñas y 3 eventos nuevos) ==")
    customers_ref, products_ref = ab.load_reference_rows(mysql.cursor())
    day = datetime(2026, 1, 1) + timedelta(days=1)
    cur = mysql.cursor()
    inserted = ab.write_orders(cur, ab.build_order_rows(2, day, customers_ref, products_ref)); mysql.commit()
    r_docs, e_docs = ab.build_mongo_documents(2, day, customers_ref, products_ref, __import__("faker").Faker("es_CO"), __import__("random").Random(2))
    mongo["ecommerce"]["reviews"].insert_many(r_docs); mongo["ecommerce"]["navigation_events"].insert_many(e_docs)
    ok(inserted == 3, "fuentes: 3 pedidos nuevos")
    b2 = "20260102T100000Z"
    m2, _ = run_batch(mysql, mongo, s3, b2)
    by = {(e["source"], e["entity"]): e for e in m2["entities"]}
    ok(by[("mysql", "orders")]["watermark_from"] is not None and 3 <= by[("mysql", "orders")]["rows"] < 106,
       f"extracción incremental de pedidos: {by[('mysql', 'orders')]['rows']} filas (los 3 nuevos + la ventana de 7 días), no los 106")
    ok(by[("mysql", "customers")]["rows"] == 0 and by[("mysql", "products")]["rows"] == 0, "clientes y productos sin cambios: 0 filas")
    dbt_pipeline(full=False)
    f2 = facts()
    ok({t: f2[t][0] for t in base} == {"fct_sales": 256, "fct_reviews": 153, "fct_navigation_events": 503}, "cada hecho creció en 3 (256 / 153 / 503)")
    ok(all(f2[t][0] == f2[t][1] for t in f2), "sin llaves duplicadas en los hechos")
    finish_batch(b2, m2)

    print("\n== Lote 3: repetición (sin cambios en las fuentes) ==")
    b3 = "20260103T100000Z"
    m3, _ = run_batch(mysql, mongo, s3, b3)
    dbt_pipeline(full=False)
    ok(facts() == f2, "repetir no cambia ningún hecho (mismas filas, llaves y contenido)")
    ok(scalar("select count(*) from raw.mysql_orders") > 106 + 3, "raw acumula los lotes (filas repetidas por la ventana)...")
    ok(scalar("select count(*) from intermediate.stg_mysql__orders") == 109, "...y staging deja una sola versión por llave (109 pedidos)")
    finish_batch(b3, m3)

    print("\n== Lote 4: cambio en atributos historizados (SCD2) ==")
    changed = datetime(2025, 7, 1)
    cur = mysql.cursor()
    cur.execute("UPDATE customers SET city=%s, department=%s, updated_at=%s WHERE customer_id=%s", ("Medellín", "Antioquia", changed, 1))
    cur.execute("UPDATE products SET category=%s, updated_at=%s WHERE product_id=%s", ("Hogar", changed, 1)); mysql.commit()
    b4 = "20260104T100000Z"
    m4, _ = run_batch(mysql, mongo, s3, b4)
    by = {(e["source"], e["entity"]): e for e in m4["entities"]}
    ok(by[("mysql", "customers")]["rows"] == 1 and by[("mysql", "products")]["rows"] == 1, "solo se extrae lo que cambió: 1 cliente y 1 producto")
    dbt_pipeline(full=False)
    versions = q("select customer_sk, city, valid_from, valid_to, is_current from marts.dim_customer where customer_id = 1 order by valid_from")
    ok(len(versions) == 2 and versions[0][4] is False and versions[1][4] is True and versions[0][3] == versions[1][2], f"cliente 1: 2 versiones; la anterior se cerró y la nueva está vigente ({[v[1] for v in versions]})")
    ok(scalar("select count(*) from marts.dim_product where product_id = 1") == 2, "producto 1: 2 versiones")
    f4 = facts()
    ok({t: f4[t][0] for t in base} == {"fct_sales": 256, "fct_reviews": 153, "fct_navigation_events": 503} and f4["fct_sales"][2] != f2["fct_sales"][2],
       "los hechos mantienen sus filas, pero los posteriores al cambio apuntan a la versión nueva de la dimensión")
    finish_batch(b4, m4)
    conn = warehouse()
    results = pipeline.reconcile(conn)
    ok(all(r["ok"] for r in results) and len(results) == 4, "conciliación del modelo con el origen válido: " + "; ".join(f"{r['indicador']}={'OK' if r['ok'] else 'FALLA'}" for r in results))
    before = control.get_watermarks(conn)
    control.advance_watermarks(conn, "20250101T000000Z", m1["entities"])  # reprocesar un lote viejo
    ok(control.get_watermarks(conn) == before, "la marca de avance solo se mueve hacia adelante: reprocesar un lote antiguo no la retrocede")
    conn.close()

    print("\n== Recuperación: fallo de COPY a mitad de la carga ==")
    ab.write_orders(mysql.cursor(), ab.build_order_rows(3, datetime(2026, 1, 3), customers_ref, products_ref)); mysql.commit()
    b5 = "20260105T100000Z"
    conn = warehouse(); control.ensure_tables(conn)
    marks = control.get_watermarks(conn)
    d = WORK / "extracts" / b5
    recs = extract.extract_mysql(mysql, d, b5, marks) + extract.extract_mongo(mongo, "ecommerce", d, b5, marks)
    manifest = s3_batches.upload_batch(s3, BUCKET, b5, recs)
    loader = DuckLoader(conn, BUCKET, "arn:aws:iam::CUENTA:role/prueba")
    loader.fail_on = "mongo_reviews"
    try:
        loader.load_batch(b5, manifest); ok(False, "debía fallar")
    except RuntimeError as e:
        ok("fallo simulado" in str(e), "el primer intento falla en mongo_reviews")
    ok(sc(conn, "select count(*) from raw.mysql_orders where _batch_id = '" + b5 + "'") > 0, "las entidades anteriores a la falla quedaron cargadas")
    loader.fail_on = None
    again = loader.load_batch(b5, s3_batches.read_batch(s3, BUCKET, b5))
    ok(all(sc(conn, f"select count(*) from raw.{BY_NAME[tuple(k.split('.'))].raw_table} where _batch_id = '{b5}'") == v for k, v in again.items()),
       "la reejecución del mismo lote reemplaza sus filas: sin duplicados y sin pérdidas")
    ok(sc(conn, "select count(*) from control.watermarks where batch_id = '" + b5 + "'") == 0, "como el lote no terminó, la marca de avance no se movió")
    conn.close()

    print("\n== S3: nunca se sobrescribe un lote ==")
    again = s3_batches.upload_batch(s3, BUCKET, b5, recs)
    ok(all(e["reused"] for e in again["entities"]), "subir de nuevo el mismo lote reutiliza los archivos (reintento seguro)")
    altered = [dict(r, sha256="0" * 64) for r in recs]
    try:
        s3_batches.upload_batch(s3, BUCKET, b5, altered); ok(False, "debía fallar")
    except RuntimeError as e:
        ok("otro contenido" in str(e), "el mismo lote con otro contenido se rechaza")

    print("\nTODAS LAS PRUEBAS PASARON")


if __name__ == "__main__":
    main()
