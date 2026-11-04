#!/usr/bin/env python3
"""Ejecuta y documenta los escenarios del data warehouse en AWS (sección 3.5 del enunciado).

Cada escenario toma una foto del almacén (conteos y huellas de los hechos) antes y después, ejecuta lo necesario en la EC2
(lotes nuevos, cambios en las fuentes, el DAG) y deja un archivo de evidencia en evidencias/escenarios_dw/.
Dura unos 35 a 50 minutos con todos los escenarios. El de fallo controlado espera los reintentos de Airflow.

Uso: python3 infra/escenarios_dw.py                         (todos)
     python3 infra/escenarios_dw.py incremental scd2        (solo esos)
Escenarios: carga_inicial defectuosos repeticion incremental scd2 fallo_carga fallo_dbt
"""
import json
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timezone

import boto3

RAIZ = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
os.chdir(RAIZ)
ENV = {}
for linea in open("recursos_dw.env", encoding="utf-8"):
    if "=" in linea and not linea.startswith("#"):
        k, v = linea.strip().split("=", 1)
        ENV[k] = v
S3 = boto3.client("s3", region_name=ENV["REGION"])
SALIDA = "evidencias/escenarios_dw"
os.makedirs(SALIDA, exist_ok=True)
CLAVE = os.path.expanduser("~/.ssh/taller-dw-key.pem")
DC = "cd ~/DE-DW && docker compose --project-directory ~/DE-DW -f docker/docker-compose.yml --env-file .env"
RESUMEN = []


def enmascarar(texto):
    return re.sub(r"\b\d{12}\b", "************", texto)


def ssh(comando, entrada=None, tiempo=1800):
    p = subprocess.run(["ssh", "-i", CLAVE, "-o", "StrictHostKeyChecking=accept-new", "-o", "ConnectTimeout=20", f"ec2-user@{ENV['EC2_IP']}", comando],
                       input=entrada, capture_output=True, text=True, timeout=tiempo)
    return p.stdout + p.stderr


def en_contenedor(comando, entrada=None):
    return ssh(f"{DC} exec -T airflow-scheduler {comando}", entrada)


def ultimo_json(texto):
    for linea in reversed(texto.splitlines()):
        linea = linea.strip()
        if linea.startswith("{"):
            try:
                return json.loads(linea)
            except ValueError:
                continue
    raise RuntimeError("No se encontró JSON en la respuesta:\n" + texto[-800:])


def sql(consulta):
    """Devuelve las filas de una consulta a Redshift como lista de listas."""
    return ultimo_json(en_contenedor("python -m ecommerce_dw.consulta --json", consulta))["rows"]


def foto():
    return ultimo_json(en_contenedor("python -m ecommerce_dw.consulta --foto"))


def ejecutar_pipeline(conf=None, minutos=40):
    rid = f"esc_{datetime.now():%m%d%H%M%S}"
    conf_json = json.dumps(conf or {})
    ssh(f"{DC} exec -T airflow-scheduler airflow dags unpause dw_pipeline")
    ssh(f"bash -s", entrada=f"{DC} exec -T airflow-scheduler airflow dags trigger dw_pipeline --run-id '{rid}' --conf '{conf_json}'\n")
    clave = f"runs/dag_run={rid}/pipeline_status.json"
    limite = time.time() + minutos * 60
    while time.time() < limite:
        try:
            return json.loads(S3.get_object(Bucket=ENV["BUCKET"], Key=clave)["Body"].read()), rid
        except S3.exceptions.NoSuchKey:
            time.sleep(8)
    raise TimeoutError(f"La ejecución {rid} no terminó en {minutos} minutos")


def lote_nuevo_en_fuentes(desde=2, intentos=12):
    """Agrega un lote diario (3 pedidos, 3 reseñas y 3 eventos) a RDS y DocumentDB; busca un número de lote sin usar."""
    for n in range(desde, desde + intentos):
        salida = en_contenedor(f"python /opt/airflow/src/ecommerce_dw/sources/append_batch.py --batch-id {n}")
        if "3 new orders" in salida:
            return n, [l for l in salida.splitlines() if l.startswith("Batch")][-1]
    raise RuntimeError("No se encontró un número de lote sin usar entre las fuentes")


def logs_tarea(rid, tarea):
    comando = (f"cd ~/DE-DW && docker compose --project-directory ~/DE-DW -f docker/docker-compose.yml --env-file .env exec -T airflow-scheduler bash -c "
               f"\"for f in \\$(find /opt/airflow/logs -path '*run_id={rid}*' -path '*task_id={tarea}*' -name 'attempt=*.log' | sort); do "
               f"echo \\\"### \\$f\\\"; grep -E 'FALLO|lote=|RuntimeError' \\$f | cut -c1-260 | tail -9; done\"")
    return ssh(comando)


class Evidencia:
    def __init__(self, archivo, nombre, esperado):
        self.archivo, self.nombre, self.esperado, self.cuerpo, self.cumple = archivo, nombre, esperado, [], True

    def linea(self, texto=""):
        self.cuerpo.append(texto)

    def comprobar(self, descripcion, condicion):
        self.cumple = self.cumple and bool(condicion)
        self.cuerpo.append(f"  [{'OK' if condicion else 'FALLA'}] {descripcion}")
        return bool(condicion)

    def cerrar(self):
        cabecera = [f"ESCENARIO: {self.nombre}", f"RESULTADO ESPERADO: {self.esperado}", f"ESTADO: {'CUMPLE' if self.cumple else 'NO CUMPLE'}",
                    f"FECHA (UTC): {datetime.now(timezone.utc):%Y-%m-%d %H:%M:%S}", ""]
        open(os.path.join(SALIDA, self.archivo), "w", encoding="utf-8").write(enmascarar("\n".join(cabecera + self.cuerpo)) + "\n")
        RESUMEN.append((self.nombre, self.cumple, self.archivo))
        print(("CUMPLE     " if self.cumple else "NO CUMPLE  ") + self.nombre)


def mostrar(ev, antes, despues):
    ev.linea("--- Estado del almacén (filas, llaves distintas, total, huella del contenido) ---")
    for k in antes:
        ev.linea(f"  {k:16} antes={antes[k]}")
        ev.linea(f"  {'':16} después={despues[k]}")


def conciliacion(lote):
    try:
        return json.loads(S3.get_object(Bucket=ENV["BUCKET"], Key=f"quality/batch_id={lote}/reconciliation.json")["Body"].read())
    except S3.exceptions.NoSuchKey:
        return None


def mostrar_conciliacion(ev, lote):
    c = conciliacion(lote)
    if not c:
        ev.comprobar("existe el informe de conciliación del lote", False)
        return
    ev.linea(f"--- Conciliación del modelo con el origen válido (s3://.../quality/batch_id={lote}/reconciliation.json) ---")
    for r in c["conciliacion"]:
        detalle = (f"origen={r['filas_origen_valido']} filas / {r['total_origen_valido']}; modelo={r['filas_modelo']} filas / {r['total_modelo']}; llaves duplicadas={r['llaves_duplicadas']}"
                   if "filas_modelo" in r else f"fechas con diferencia={r['fechas_con_diferencia']}")
        ev.comprobar(f"{r['indicador']}: {detalle}", r["ok"])
    ev.linea(f"  Pruebas de dbt: {c['dbt_test']}")


def estado_ok(ev, estado, rid):
    ev.linea(f"Ejecución de Airflow: {rid} -> {estado['estado']} | lote {estado['batch_id']} | modo {estado['mode']}")
    ev.comprobar("la ejecución terminó EXITOSA y todas las tareas en success", estado["estado"] == "EXITOSO" and all(v == "success" for v in estado["tareas"].values()))


# ------------------------------------------------------------------------------------------------ escenarios
def esc_carga_inicial():
    ev = Evidencia("01_carga_inicial.txt", "Carga inicial", "Todas las entidades cargadas desde el origen, conciliadas y consultables en el modelo")
    primero = sql("select batch_id from control.batches where status = 'COMPLETO' order by batch_id limit 1")[0][0]
    ev.linea(f"Primer lote COMPLETO: {primero}")
    ents = sql(f"select source, entity, rows_extracted, rows_loaded, coalesce(watermark_from, '(sin marca previa)') from control.batch_entities where batch_id = '{primero}' order by source, entity")
    ev.linea("--- Entidades del lote (control.batch_entities) ---")
    for fila in ents:
        ev.linea(f"  {fila[0]}.{fila[1]:18} extraídas={fila[2]:>4}  cargadas en raw={fila[3]:>4}  ventana desde: {fila[4]}")
    ev.comprobar("carga inicial: ninguna entidad tenía marca de avance, se extrajo todo", all(f[4] == "(sin marca previa)" for f in ents))
    ev.comprobar("lo extraído coincide con lo cargado en raw en cada entidad", all(f[2] == f[3] for f in ents))
    mostrar_conciliacion(ev, primero)
    f = foto()
    ev.linea(f"Estado actual del modelo: {json.dumps(f)}")
    ev.comprobar("los tres hechos y las dimensiones tienen datos", f["ventas"][0] > 0 and f["resenas"][0] > 0 and f["eventos"][0] > 0 and f["dim_customer"][0] > 0)
    ev.cerrar()


def esc_defectuosos():
    ev = Evidencia("02_registros_defectuosos.txt", "Registros defectuosos", "Los rechazos quedan en cuarentena con su motivo y su lote de origen, y no entran al modelo")
    filas = sql("""select entidad, stg, valido, cuarentena from (
        select 'customers' as entidad, (select count(*) from intermediate.stg_mysql__customers) stg, (select count(*) from intermediate.int_mysql__customers__valid) valido, (select count(*) from quarantine.quarantine_mysql__customers) cuarentena
        union all select 'products', (select count(*) from intermediate.stg_mysql__products), (select count(*) from intermediate.int_mysql__products__valid), (select count(*) from quarantine.quarantine_mysql__products)
        union all select 'orders', (select count(*) from intermediate.stg_mysql__orders), (select count(*) from intermediate.int_mysql__orders__valid), (select count(*) from quarantine.quarantine_mysql__orders)
        union all select 'order_items', (select count(*) from intermediate.stg_mysql__order_items), (select count(*) from intermediate.int_mysql__order_items__valid), (select count(*) from quarantine.quarantine_mysql__order_items)
        union all select 'reviews', (select count(*) from intermediate.stg_mongodb__reviews), (select count(*) from intermediate.int_mongodb__reviews__valid), (select count(*) from quarantine.quarantine_mongodb__reviews)
        union all select 'navigation_events', (select count(*) from intermediate.stg_mongodb__navigation_events), (select count(*) from intermediate.int_mongodb__navigation_events__valid), (select count(*) from quarantine.quarantine_mongodb__navigation_events)) x order by 1""")
    ev.linea("--- Cada registro recibido es válido o está en cuarentena ---")
    for e, stg, ok, q in filas:
        ev.comprobar(f"{e:18} recibidos={stg:>4} = válidos {ok:>4} + cuarentena {q:>3}", stg == ok + q)
    motivos = sql("""select tabla, motivo, lote, n from (
        select 'customers' tabla, rejection_reason motivo, _batch_id lote, count(*) n from quarantine.quarantine_mysql__customers group by 2, 3
        union all select 'products', rejection_reason, _batch_id, count(*) from quarantine.quarantine_mysql__products group by 2, 3
        union all select 'orders', rejection_reason, _batch_id, count(*) from quarantine.quarantine_mysql__orders group by 2, 3
        union all select 'order_items', rejection_reason, _batch_id, count(*) from quarantine.quarantine_mysql__order_items group by 2, 3
        union all select 'reviews', rejection_reason, _batch_id, count(*) from quarantine.quarantine_mongodb__reviews group by 2, 3
        union all select 'navigation_events', rejection_reason, _batch_id, count(*) from quarantine.quarantine_mongodb__navigation_events group by 2, 3) x order by 1, 2""")
    ev.linea("--- Cuarentena por entidad, motivo y lote de origen ---")
    for t, m, l, n in motivos:
        ev.linea(f"  {t:18} {m:24} lote de origen {l}   {n} registros")
    ev.comprobar("hay motivos de rechazo registrados y todos tienen lote de origen", len(motivos) > 0 and all(m[2] for m in motivos))
    f = foto()
    cuarentena_total = sum(m[3] for m in motivos)
    ev.comprobar(f"el total de la cuarentena ({cuarentena_total}) coincide con la foto del almacén ({f['cuarentena'][0]})", cuarentena_total == f["cuarentena"][0])
    ev.comprobar("los rechazados no están en los hechos: ventas = líneas de pedido válidas", sql("select count(*) from intermediate.int_mysql__order_items__valid")[0][0] == f["ventas"][0])
    ev.cerrar()


def esc_repeticion():
    ev = Evidencia("03_repeticion_lote_exitoso.txt", "Repetición de un lote exitoso", "Sin duplicación de hechos ni de versiones históricas")
    lote = sql("select batch_id from control.batches where status = 'COMPLETO' order by batch_id desc limit 1")[0][0]
    ev.linea(f"Se reprocesa desde S3, sin consultar las fuentes, el lote ya exitoso {lote}")
    antes = foto()
    raw_antes = sql(f"select count(*) from \"raw\".mysql_orders where _batch_id = '{lote}'")[0][0]
    marcas_antes = sql("select source, entity, watermark_value from control.watermarks order by 1, 2")
    estado, rid = ejecutar_pipeline({"reprocess_batch_id": lote})
    despues = foto()
    estado_ok(ev, estado, rid)
    mostrar(ev, antes, despues)
    ev.comprobar("modo REPROCESO: no se consultaron las fuentes", estado["mode"] == "REPROCESO")
    ev.comprobar("hechos idénticos: mismas filas, llaves, totales y huella de contenido", all(antes[k] == despues[k] for k in ("ventas", "resenas", "eventos")))
    ev.comprobar("versiones de dimensiones iguales (sin versiones históricas nuevas)", antes["dim_customer"] == despues["dim_customer"] and antes["dim_product"] == despues["dim_product"])
    ev.comprobar("la cuarentena no cambió", antes["cuarentena"] == despues["cuarentena"])
    ev.comprobar("raw reemplaza las filas del lote, no las duplica", sql(f"select count(*) from \"raw\".mysql_orders where _batch_id = '{lote}'")[0][0] == raw_antes)
    ev.comprobar("las marcas de avance no se movieron", sql("select source, entity, watermark_value from control.watermarks order by 1, 2") == marcas_antes)
    ev.cerrar()


def esc_incremental():
    ev = Evidencia("04_nuevo_lote_incremental.txt", "Nuevo lote incremental", "Solo se extraen y procesan los datos nuevos; los hechos crecen exactamente con ellos")
    antes = foto()
    n, texto = lote_nuevo_en_fuentes()
    ev.linea(f"Fuentes: {texto}")
    estado, rid = ejecutar_pipeline()
    despues = foto()
    estado_ok(ev, estado, rid)
    lote = estado["batch_id"]
    ents = sql(f"select source, entity, rows_extracted, coalesce(watermark_from, '-') from control.batch_entities where batch_id = '{lote}' order by 1, 2")
    ev.linea("--- Extracción incremental (control.batch_entities) ---")
    for fila in ents:
        ev.linea(f"  {fila[0]}.{fila[1]:18} extraídas={fila[2]:>4}  ventana desde: {fila[3]}")
    mostrar(ev, antes, despues)
    por_hecho = {k: despues[k][0] - antes[k][0] for k in ("ventas", "resenas", "eventos")}
    ev.comprobar(f"cada hecho creció exactamente en 3 filas: {por_hecho}", all(v == 3 for v in por_hecho.values()))
    ev.comprobar("la extracción fue incremental: ninguna entidad con marca de avance trajo la tabla completa", all(f[3] != "-" for f in ents if f[2] > 0 and f[1] in ("orders", "reviews", "navigation_events")))
    ev.comprobar("sin llaves duplicadas en los hechos", all(despues[k][0] == despues[k][1] for k in ("ventas", "resenas", "eventos")))
    mostrar_conciliacion(ev, lote)
    ev.cerrar()


def esc_scd2():
    ev = Evidencia("05_cambio_atributo_historizado.txt", "Cambio en un atributo historizado (SCD2)", "Una versión nueva de la dimensión; la anterior se cierra; los hechos posteriores apuntan a la nueva")
    cliente = sql("select customer_id from marts.dim_customer group by customer_id having count(*) = 1 order by customer_id limit 1 offset 2")[0][0]
    producto = sql("select product_id from marts.dim_product group by product_id having count(*) = 1 order by product_id limit 1 offset 2")[0][0]
    city, dep = sql(f"select city, department from marts.dim_customer where customer_id = {cliente} and is_current")[0]
    cat = sql(f"select category from marts.dim_product where product_id = {producto} and is_current")[0][0]
    nueva_city, nueva_dep = ("Cali", "Valle del Cauca") if city != "Cali" else ("Medellín", "Antioquia")
    nueva_cat = "Deportes" if cat != "Deportes" else "Hogar"
    fecha = "2025-09-01T00:00:00"
    ev.linea(f"Cliente {cliente}: {city}, {dep} -> {nueva_city}, {nueva_dep}   |   Producto {producto}: {cat} -> {nueva_cat}   |   vigente desde {fecha}")
    antes = foto()
    ventas_antes = sql(f"select count(*) from marts.fct_sales s join marts.dim_customer c on c.customer_sk = s.customer_sk where c.customer_id = {cliente}")[0][0]
    salida = en_contenedor(f"python /opt/airflow/src/ecommerce_dw/sources/simulate_changes.py --customer-id {cliente} --product-id {producto} --city '{nueva_city}' --department '{nueva_dep}' --category {nueva_cat} --changed-at {fecha}")
    ev.linea("Fuente: " + " | ".join(l for l in salida.splitlines() if l.startswith(("Customer", "Product"))))
    estado, rid = ejecutar_pipeline()
    despues = foto()
    estado_ok(ev, estado, rid)
    versiones = sql(f"select customer_sk, city, valid_from, valid_to, is_current from marts.dim_customer where customer_id = {cliente} order by valid_from")
    ev.linea("--- Versiones del cliente (marts.dim_customer) ---")
    for v in versiones:
        ev.linea(f"  {v[0][:10]}…  {v[1]:14} desde {v[2]}  hasta {v[3]}  vigente={v[4]}")
    pversiones = sql(f"select product_sk, category, valid_from, valid_to, is_current from marts.dim_product where product_id = {producto} order by valid_from")
    ev.linea("--- Versiones del producto (marts.dim_product) ---")
    for v in pversiones:
        ev.linea(f"  {v[0][:10]}…  {v[1]:14} desde {v[2]}  hasta {v[3]}  vigente={v[4]}")
    ev.comprobar("el cliente tiene 2 versiones: la anterior cerrada y la nueva vigente", len(versiones) == 2 and versiones[0][4] is False and versiones[1][4] is True and versiones[0][3] == versiones[1][2])
    ev.comprobar("el producto tiene 2 versiones: la anterior cerrada y la nueva vigente", len(pversiones) == 2 and pversiones[0][4] is False and pversiones[1][4] is True)
    reparto = sql(f"select c.is_current, count(*) from marts.fct_sales s join marts.dim_customer c on c.customer_sk = s.customer_sk where c.customer_id = {cliente} group by 1 order by 1")
    ev.linea(f"--- Ventas del cliente por versión (vigente=False/True): {reparto} ---")
    total = sum(r[1] for r in reparto)
    ev.comprobar(f"las ventas del cliente no se pierden ni se duplican: antes {ventas_antes}, después {total}", total == ventas_antes)
    ev.comprobar("el número de filas de los hechos no cambió", all(antes[k][0] == despues[k][0] for k in ("ventas", "resenas", "eventos")))
    ev.comprobar("la dimensión creció en una versión por cada entidad cambiada", despues["dim_customer"][0] == antes["dim_customer"][0] + 1 and despues["dim_product"][0] == antes["dim_product"][0] + 1)
    mostrar_conciliacion(ev, estado["batch_id"])
    ev.cerrar()


def esc_fallo(archivo, nombre, parametro, tarea):
    ev = Evidencia(archivo, nombre, "Recuperación sin pérdida ni duplicación: el lote se completa en el reintento")
    antes = foto()
    n, texto = lote_nuevo_en_fuentes()
    ev.linea(f"Fuentes: {texto}. Se provoca UN fallo controlado (simular_fallo={parametro}).")
    estado, rid = ejecutar_pipeline({"simular_fallo": parametro})
    despues = foto()
    estado_ok(ev, estado, rid)
    lote = estado["batch_id"]
    ev.linea(f"--- Registros de la tarea {tarea}, un archivo por intento (se ven el lote y la tarea en cada línea) ---")
    registros = logs_tarea(rid, tarea)
    ev.linea(registros)
    ev.linea("--- Estado del lote en control.batches ---")
    ev.linea("  " + str(sql(f"select batch_id, status, detail from control.batches where batch_id = '{lote}'")))
    ents = sql(f"select source, entity, rows_extracted, rows_loaded from control.batch_entities where batch_id = '{lote}' order by 1, 2")
    mostrar(ev, antes, despues)
    ev.comprobar("el primer intento falló a propósito", "FALLO SIMULADO" in registros)
    ev.comprobar("hubo un segundo intento y el lote terminó COMPLETO", registros.count("### ") >= 2 and sql(f"select status from control.batches where batch_id = '{lote}'")[0][0] == "COMPLETO")
    ev.comprobar("raw quedó con las filas extraídas, sin duplicados ni pérdidas", all(e[2] == e[3] for e in ents))
    por_hecho = {k: despues[k][0] - antes[k][0] for k in ("ventas", "resenas", "eventos")}
    ev.comprobar(f"los hechos crecieron exactamente con el lote nuevo: {por_hecho}", all(v == 3 for v in por_hecho.values()))
    ev.comprobar("sin llaves duplicadas en los hechos", all(despues[k][0] == despues[k][1] for k in ("ventas", "resenas", "eventos")))
    mostrar_conciliacion(ev, lote)
    ev.cerrar()


ESCENARIOS = {
    "carga_inicial": esc_carga_inicial, "defectuosos": esc_defectuosos, "repeticion": esc_repeticion,
    "incremental": esc_incremental, "scd2": esc_scd2,
    "fallo_carga": lambda: esc_fallo("06a_fallo_carga_redshift.txt", "Fallo controlado y reejecución (carga a Redshift)", "carga_redshift", "4_cargar_redshift"),
    "fallo_dbt": lambda: esc_fallo("06b_fallo_dbt_marts.txt", "Fallo controlado y reejecución (transformación dbt)", "dbt_marts", "5_dbt_marts"),
}

if __name__ == "__main__":
    elegidos = sys.argv[1:] or list(ESCENARIOS)
    for nombre in elegidos:
        if nombre not in ESCENARIOS:
            sys.exit(f"Escenario desconocido: {nombre}. Opciones: {', '.join(ESCENARIOS)}")
    for nombre in elegidos:
        print(f"\n>>> {nombre}", flush=True)
        try:
            ESCENARIOS[nombre]()
        except Exception as error:  # un escenario que falla no debe impedir los demás
            print(f"ERROR en {nombre}: {error}")
            RESUMEN.append((nombre, False, f"ERROR: {error}"))
    # El resumen se reconstruye siempre desde los archivos de evidencia: correr un solo escenario no deja líneas repetidas.
    lineas = ["RESUMEN DE ESCENARIOS DEL DATA WAREHOUSE", ""]
    for archivo in sorted(f for f in os.listdir(SALIDA) if re.match(r"0\d.*\.txt$", f)):
        cabecera = open(os.path.join(SALIDA, archivo), encoding="utf-8").read().splitlines()[:3]
        estado = cabecera[2].split(": ", 1)[1] if len(cabecera) > 2 else "?"
        lineas.append(f"{'CUMPLE   ' if estado == 'CUMPLE' else 'NO CUMPLE'}  {cabecera[0].split(': ', 1)[1]}  ({archivo})")
    lineas += [f"NO CUMPLE  {n}  ({a})" for n, c, a in RESUMEN if str(a).startswith("ERROR")]
    texto = enmascarar("\n".join(lineas)) + "\n"
    open(os.path.join(SALIDA, "RESUMEN.txt"), "w", encoding="utf-8").write(texto)
    print("\n" + texto)
