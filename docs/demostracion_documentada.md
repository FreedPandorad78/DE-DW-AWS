# Demostración documentada del data warehouse en AWS

Este documento recorre la demostración que pide la sección 4 de los entregables del data warehouse. Es una demostración **escrita**: explica qué se ejecuta, cómo repetirlo y muestra extractos reales de las evidencias generadas al ejecutarlo (los archivos completos están en `evidencias/dw/` y `evidencias/escenarios_dw/`).

**Alcance:** QuickSight quedó fuera por indicación del profesor, así que «hasta el dashboard» se entiende como hasta las tablas del modelo analítico (`marts`) y la conciliación de un indicador con su consulta SQL.

## 1. Procesar un nuevo lote desde las fuentes hasta el modelo analítico

Se agrega un lote diario a las fuentes (3 pedidos, 3 reseñas y 3 eventos en RDS MySQL y DocumentDB) y se ejecuta el DAG `dw_pipeline`, de 7 pasos y sin intervención manual: (1) validar conexiones y parámetros; (2) extraer de forma **incremental** desde la marca de avance, más una ventana de 7 días para datos tardíos; (3) escribir el lote en S3 (`raw/<fuente>/<entidad>/batch_id=<id>/`, con manifiesto, sin sobrescribir); (4) cargar en Redshift con `COPY`, verificando los conteos; (5) transformar con dbt (staging, calidad con cuarentena, snapshots SCD2 y marts); (6) pruebas de calidad y conciliación; (7) registrar el resultado. Los hechos crecen exactamente con lo nuevo, y un indicador (ventas y líneas por mes) se concilia entre el origen válido y el modelo con una consulta SQL.

**Cómo se repite:**

```bash
./infra/agregar_lote_dw.sh 40
./infra/ejecutar_pipeline_dw.sh
./infra/consulta_dw.sh --resumen
python3 infra/evidencias_dw.py indicador
```

**Evidencia:** Escenario: nuevo lote incremental, antes y después — `evidencias/escenarios_dw/04_nuevo_lote_incremental.txt`

```text
ESCENARIO: Nuevo lote incremental
RESULTADO ESPERADO: Solo se extraen y procesan los datos nuevos; los hechos crecen exactamente con ellos
ESTADO: CUMPLE
FECHA (UTC): 2026-11-04 18:08:09

Fuentes: Batch 2 (2026-01-02): 3 new orders, 3 reviews and 3 navigation events upserted
Ejecución de Airflow: esc_1104130550 -> EXITOSO | lote 20261104T180600Z | modo NUEVA_EXTRACCION
  [OK] la ejecución terminó EXITOSA y todas las tareas en success
--- Extracción incremental (control.batch_entities) ---
  mongodb.navigation_events  extraídas=  13  ventana desde: 2025-12-24T14:28:00+00:00
  mongodb.reviews            extraídas=   7  ventana desde: 2025-12-23T19:19:00+00:00
  mysql.customers          extraídas=   0  ventana desde: 2025-01-01 00:00:00
  mysql.order_items        extraídas=   9  ventana desde: 2025-12-22 01:42:00
  mysql.orders             extraídas=   5  ventana desde: 2025-12-22 01:42:00
  mysql.products           extraídas=   0  ventana desde: 2025-01-01 00:00:00
--- Estado del almacén (filas, llaves distintas, total, huella del contenido) ---
  ventas           antes=[253, 253, '7293790.26', ************]
                   después=[256, 256, '7357260.42', ************]
  resenas          antes=[150, 150, '477', ************]
                   después=[153, 153, '483', ************]
  eventos          antes=[500, 500, '0', 1051341798984]
                   después=[503, 503, '0', 1054875194570]
  dim_customer     antes=[50, 50]
                   después=[50, 50]
  dim_product      antes=[25, 25]
                   después=[25, 25]
  cuarentena       antes=[32]
                   después=[32]
  lotes_completos  antes=[2]
                   después=[3]
  [OK] cada hecho creció exactamente en 3 filas: {'ventas': 3, 'resenas': 3, 'eventos': 3}
  [OK] la extracción fue incremental: ninguna entidad con marca de avance trajo la tabla completa
  [OK] sin llaves duplicadas en los hechos
--- Conciliación del modelo con el origen válido (s3://.../quality/batch_id=20261104T180600Z/reconciliation.json) ---
  [OK] ventas: origen=256 filas / 7357260.42; modelo=256 filas / 7357260.42; llaves duplicadas=0
  [OK] reseñas: origen=153 filas / 0; modelo=153 filas / 0; llaves duplicadas=0
  [OK] eventos de navegación: origen=503 filas / 0; modelo=503 filas / 0; llaves duplicadas=0
  [OK] subtotal de ventas por fecha de negocio: fechas con diferencia=0
  Pruebas de dbt: {'pass': 92, 'warn': 0, 'error': 0, 'skip': 0, 'total': 92}
```

**Evidencia:** Estado de una ejecución exitosa: las tareas y las filas cargadas — `evidencias/dw/airflow_evidencia.txt`

```text
== 3. Última ejecución exitosa: esc_1104131527 (lote 20261104T181537Z) ==
estado: EXITOSO   modo: NUEVA_EXTRACCION   finalizó: 2026-11-04T18:18:49+00:00
tareas: {
  "4_cargar_redshift": "success",
  "5_dbt_deps": "success",
  "5_dbt_seed": "success",
  "5_dbt_intermediate": "success",
  "1_validar_conexiones_y_parametros": "success",
  "5_dbt_quarantine": "success",
  "2_extraer_mysql": "success",
  "2_extraer_mongodb": "success",
  "3_escribir_s3": "success",
  "5_dbt_snapshot": "success",
  "6_pruebas_calidad": "success",
  "5_dbt_marts": "success"
}
filas cargadas en raw: {"mysql.customers": 0, "mysql.products": 0, "mysql.orders": 10, "mysql.order_items": 13, "mongodb.reviews": 10, "mongodb.navigation_events": 15}
pruebas y conciliación: {"dbt_test": {"pass": 92, "warn": 0, "error": 0, "skip": 0, "total": 92}, "conciliacion_ok": true}

```

**Evidencia:** Hechos en Redshift — `evidencias/dw/redshift_estado.txt`

```text
== Hechos (marts)
hecho                 | filas | llaves_distintas | total     
----------------------+-------+------------------+-----------
fct_navigation_events | 509   | 509              | -         
fct_sales             | 262   | 262              | 7484200.74
fct_reviews           | 159   | 159              | -         
(3 filas)

```

**Evidencia:** Conciliación del indicador entre el origen válido y el modelo — `evidencias/dw/indicador_ventas.txt`

```text
== Conciliación: meses cuya venta o cantidad de líneas difiere entre el origen y el modelo ==
select count(*) as meses_con_diferencia from (
    select coalesce(m.anio, s.anio) as anio, coalesce(m.mes, s.mes) as mes
    from (select cal.year as anio, cal.month as mes, count(*) as lineas, sum(f.subtotal) as ventas
from marts.fct_sales f join marts.dim_date cal on cal.date_sk = f.date_sk
group by cal.year, cal.month) m
    full outer join (select cast(extract(year from o.order_date) as integer) as anio, cast(extract(month from o.order_date) as integer) as mes,
       count(*) as lineas, sum(i.subtotal) as ventas
from intermediate.int_mysql__order_items__valid i join intermediate.int_mysql__orders__valid o on o.order_id = i.order_id
group by 1, 2) s on m.anio = s.anio and m.mes = s.mes
    where m.ventas is null or s.ventas is null or m.ventas <> s.ventas or m.lineas <> s.lineas) d

... (continúa en el archivo completo)
```

## 2. Cómo se identifica y se recupera una ejecución fallida

**Identificación.** Cada lote tiene un identificador (`AAAAMMDDTHHMMSSZ`) y un estado en `control.batches`: `INICIADO`, `EN_S3`, `CARGADO`, `TRANSFORMADO`, `COMPLETO` o `FALLIDO`. Una ejecución fallida se ve en la interfaz de Airflow (la tarea en rojo), en `runs/dag_run=<ejecución>/pipeline_status.json` en S3 (estado de cada tarea), en `control.batches` (estado `FALLIDO` y detalle) y en los registros, cuyas líneas llevan `[lote=...] [tarea=...]`.

**Recuperación.** Airflow reintenta hasta 2 veces con el **mismo lote**. La carga a Redshift reemplaza las filas de ese lote en `raw` (no las duplica) y dbt reconstruye desde `raw`. La marca de avance solo se mueve cuando el lote termina `COMPLETO`, después de las pruebas: si algo falla antes, el siguiente lote vuelve a pedir la misma ventana y no se pierde nada. Si no se recupera solo, se reejecuta el mismo lote o se reprocesa desde S3 con `reprocess_batch_id`, sin consultar las fuentes.

**Cómo se repite:**

```bash
./infra/ejecutar_pipeline_dw.sh '{"simular_fallo": "carga_redshift"}'    # fallo controlado: el primer intento falla y el segundo completa
```

**Evidencia:** Fallo controlado en la carga a Redshift — `evidencias/escenarios_dw/06a_fallo_carga_redshift.txt`

```text
ESCENARIO: Fallo controlado y reejecución (carga a Redshift)
RESULTADO ESPERADO: Recuperación sin pérdida ni duplicación: el lote se completa en el reintento
ESTADO: CUMPLE
FECHA (UTC): 2026-11-04 18:15:14

Fuentes: Batch 3 (2026-01-03): 3 new orders, 3 reviews and 3 navigation events upserted. Se provoca UN fallo controlado (simular_fallo=carga_redshift).
Ejecución de Airflow: esc_1104131111 -> EXITOSO | lote 20261104T181122Z | modo NUEVA_EXTRACCION
  [OK] la ejecución terminó EXITOSA y todas las tareas en success
--- Registros de la tarea 4_cargar_redshift, un archivo por intento (se ven el lote y la tarea en cada línea) ---
### /opt/airflow/logs/dag_id=dw_pipeline/run_id=esc_1104131111/task_id=4_cargar_redshift/attempt=1.log
[2026-11-04T18:11:35.510+0000] {load_redshift.py:62} INFO - lote=20261104T181122Z mysql.customers: 0 filas cargadas en raw.mysql_customers
[2026-11-04T18:11:36.592+0000] {load_redshift.py:62} INFO - lote=20261104T181122Z mysql.products: 0 filas cargadas en raw.mysql_products
    raise RuntimeError(f"FALLO SIMULADO tras cargar {len(loaded)} entidades: las cargadas ya están en raw; "
RuntimeError: FALLO SIMULADO tras cargar 2 entidades: las cargadas ya están en raw; la reejecución del lote reemplaza sus filas sin duplicar
[2026-11-04T18:11:36.692+0000] {standard_task_runner.py:124} ERROR - Failed to execute job 96 for task 4_cargar_redshift (FALLO SIMULADO tras cargar 2 entidades: las cargadas ya están en raw; la reejecución del lote reemplaza sus filas sin duplicar; 2340)
    raise RuntimeError(f"FALLO SIMULADO tras cargar {len(loaded)} entidades: las cargadas ya están en raw; "
RuntimeError: FALLO SIMULADO tras cargar 2 entidades: las cargadas ya están en raw; la reejecución del lote reemplaza sus filas sin duplicar
### /opt/airflow/logs/dag_id=dw_pipeline/run_id=esc_1104131111/task_id=4_cargar_redshift/attempt=2.log
[2026-11-04T18:13:09.029+0000] {load_redshift.py:62} INFO - lote=20261104T181122Z mysql.customers: 0 filas cargadas en raw.mysql_customers
[2026-11-04T18:13:09.967+0000] {load_redshift.py:62} INFO - lote=20261104T181122Z mysql.products: 0 filas cargadas en raw.mysql_products
[2026-11-04T18:13:11.606+0000] {load_redshift.py:62} INFO - lote=20261104T181122Z mysql.orders: 7 filas cargadas en raw.mysql_orders
[2026-11-04T18:13:13.062+0000] {load_redshift.py:62} INFO - lote=20261104T181122Z mysql.order_items: 10 filas cargadas en raw.mysql_order_items
[2026-11-04T18:13:15.182+0000] {load_redshift.py:62} INFO - lote=20261104T181122Z mongodb.reviews: 8 filas cargadas en raw.mongo_reviews
[2026-11-04T18:13:16.972+0000] {load_redshift.py:62} INFO - lote=20261104T181122Z mongodb.navigation_events: 14 filas cargadas en raw.mongo_navigation_events
[2026-11-04T18:13:17.432+0000] {logging_utils.py:14} INFO - [lote=20261104T181122Z] [tarea=4_cargar_redshift] filas cargadas en raw: {'mysql.customers': 0, 'mysql.products': 0, 'mysql.orders': 7, 'mysql.order_items': 10, 'mongodb.reviews': 8, 'mongodb.navigati

--- Estado del lote en control.batches ---
  [['20261104T181122Z', 'COMPLETO', None]]
--- Estado del almacén (filas, llaves distintas, total, huella del contenido) ---
  ventas           antes=[256, 256, '7357260.42', ************]
                   después=[259, 259, '7420730.58', ************]
  resenas          antes=[153, 153, '483', ************]
                   después=[156, 156, '493', ************]
  eventos          antes=[503, 503, '0', 1059460949802]
                   después=[506, 506, '0', 1060647705276]
  dim_customer     antes=[51, 50]
                   después=[51, 50]
  dim_product      antes=[26, 25]
                   después=[26, 25]
  cuarentena       antes=[32]
                   después=[32]
  lotes_completos  antes=[4]
                   después=[5]
  [OK] el primer intento falló a propósito
  [OK] hubo un segundo intento y el lote terminó COMPLETO
  [OK] raw quedó con las filas extraídas, sin duplicados ni pérdidas
  [OK] los hechos crecieron exactamente con el lote nuevo: {'ventas': 3, 'resenas': 3, 'eventos': 3}
  [OK] sin llaves duplicadas en los hechos
--- Conciliación del modelo con el origen válido (s3://.../quality/batch_id=20261104T181122Z/reconciliation.json) ---
  [OK] ventas: origen=259 filas / 7420730.58; modelo=259 filas / 7420730.58; llaves duplicadas=0
... (continúa en el archivo completo)
```

**Evidencia:** Fallo controlado en la transformación con dbt — `evidencias/escenarios_dw/06b_fallo_dbt_marts.txt`

```text
ESCENARIO: Fallo controlado y reejecución (transformación dbt)
RESULTADO ESPERADO: Recuperación sin pérdida ni duplicación: el lote se completa en el reintento
ESTADO: CUMPLE
FECHA (UTC): 2026-11-04 18:19:06

Fuentes: Batch 4 (2026-01-04): 3 new orders, 3 reviews and 3 navigation events upserted. Se provoca UN fallo controlado (simular_fallo=dbt_marts).
Ejecución de Airflow: esc_1104131527 -> EXITOSO | lote 20261104T181537Z | modo NUEVA_EXTRACCION
  [OK] la ejecución terminó EXITOSA y todas las tareas en success
--- Registros de la tarea 5_dbt_marts, un archivo por intento (se ven el lote y la tarea en cada línea) ---
### /opt/airflow/logs/dag_id=dw_pipeline/run_id=esc_1104131527/task_id=5_dbt_marts/attempt=1.log
    raise RuntimeError(f"FALLO SIMULADO antes de dbt {command} {selector}: raw ya está cargado; el reintento continúa desde aquí")
RuntimeError: FALLO SIMULADO antes de dbt run path:models/marts: raw ya está cargado; el reintento continúa desde aquí
[2026-11-04T18:16:53.000+0000] {standard_task_runner.py:124} ERROR - Failed to execute job 116 for task 5_dbt_marts (FALLO SIMULADO antes de dbt run path:models/marts: raw ya está cargado; el reintento continúa desde aquí; 2702)
    raise RuntimeError(f"FALLO SIMULADO antes de dbt {command} {selector}: raw ya está cargado; el reintento continúa desde aquí")
RuntimeError: FALLO SIMULADO antes de dbt run path:models/marts: raw ya está cargado; el reintento continúa desde aquí
### /opt/airflow/logs/dag_id=dw_pipeline/run_id=esc_1104131527/task_id=5_dbt_marts/attempt=2.log
[2026-11-04T18:18:27.466+0000] {dbt_runner.py:30} INFO - [lote=20261104T181537Z] dbt| [0m18:18:26  8 of 9 OK created sql incremental model marts.fct_reviews ...................... [[32mSUCCESS[0m in 6.54s]
[2026-11-04T18:18:27.466+0000] {dbt_runner.py:30} INFO - [lote=20261104T181537Z] dbt| [0m18:18:26  9 of 9 OK created sql incremental model marts.fct_sales ........................ [[32mSUCCESS[0m in 6.81s]
[2026-11-04T18:18:27.466+0000] {dbt_runner.py:30} INFO - [lote=20261104T181537Z] dbt| [0m18:18:26  
[2026-11-04T18:18:27.467+0000] {dbt_runner.py:30} INFO - [lote=20261104T181537Z] dbt| [0m18:18:26  Finished running 3 incremental models, 6 table models in 0 hours 0 minutes and 13.38 seconds (13.38s).
[2026-11-04T18:18:27.467+0000] {dbt_runner.py:30} INFO - [lote=20261104T181537Z] dbt| [0m18:18:26  
[2026-11-04T18:18:27.467+0000] {dbt_runner.py:30} INFO - [lote=20261104T181537Z] dbt| [0m18:18:26  [32mCompleted successfully[0m
[2026-11-04T18:18:27.467+0000] {dbt_runner.py:30} INFO - [lote=20261104T181537Z] dbt| [0m18:18:26  
[2026-11-04T18:18:27.467+0000] {dbt_runner.py:30} INFO - [lote=20261104T181537Z] dbt| [0m18:18:26  Done. PASS=9 WARN=0 ERROR=0 SKIP=0 TOTAL=9
[2026-11-04T18:18:27.467+0000] {logging_utils.py:14} INFO - [lote=20261104T181537Z] [tarea=5_dbt_run] resultado: {'pass': 9, 'warn': 0, 'error': 0, 'skip': 0, 'total': 9}

--- Estado del lote en control.batches ---
  [['20261104T181537Z', 'COMPLETO', None]]
--- Estado del almacén (filas, llaves distintas, total, huella del contenido) ---
  ventas           antes=[259, 259, '7420730.58', ************]
                   después=[262, 262, '7484200.74', ************]
  resenas          antes=[156, 156, '493', ************]
                   después=[159, 159, '500', ************]
  eventos          antes=[506, 506, '0', 1060647705276]
                   después=[509, 509, '0', 1066697110931]
  dim_customer     antes=[51, 50]
                   después=[51, 50]
  dim_product      antes=[26, 25]
                   después=[26, 25]
  cuarentena       antes=[32]
                   después=[32]
  lotes_completos  antes=[5]
                   después=[6]
  [OK] el primer intento falló a propósito
  [OK] hubo un segundo intento y el lote terminó COMPLETO
  [OK] raw quedó con las filas extraídas, sin duplicados ni pérdidas
  [OK] los hechos crecieron exactamente con el lote nuevo: {'ventas': 3, 'resenas': 3, 'eventos': 3}
  [OK] sin llaves duplicadas en los hechos
--- Conciliación del modelo con el origen válido (s3://.../quality/batch_id=20261104T181537Z/reconciliation.json) ---
  [OK] ventas: origen=262 filas / 7484200.74; modelo=262 filas / 7484200.74; llaves duplicadas=0
... (continúa en el archivo completo)
```

**Evidencia:** Estados de los lotes en control.batches (incluye lotes FALLIDO de las primeras corridas) — `evidencias/dw/redshift_estado.txt`

```text
== Lotes registrados (control.batches)
batch_id         | mode             | status   | inicio   | fin     
-----------------+------------------+----------+----------+---------
20261104T171111Z | NUEVA_EXTRACCION | FALLIDO  | 17:11:14 | 17:17:05
20261104T174047Z | NUEVA_EXTRACCION | FALLIDO  | 17:40:50 | 17:48:03
20261104T175208Z | NUEVA_EXTRACCION | COMPLETO | 17:52:11 | 17:54:20
20261104T175540Z | NUEVA_EXTRACCION | COMPLETO | 17:55:42 | 18:05:33
20261104T180600Z | NUEVA_EXTRACCION | COMPLETO | 18:06:04 | 18:08:00
20261104T180840Z | NUEVA_EXTRACCION | COMPLETO | 18:08:42 | 18:10:45
20261104T181122Z | NUEVA_EXTRACCION | COMPLETO | 18:11:24 | 18:14:56
... (continúa en el archivo completo)
```

## 3. Otros escenarios validados

Además de lo pedido, se validaron la carga inicial, los registros defectuosos (cuarentena con motivo y lote de origen), la repetición de un lote exitoso (sin duplicar hechos ni versiones históricas) y el cambio en un atributo historizado (SCD tipo 2: la versión anterior se cierra y los hechos posteriores apuntan a la nueva).

**Cómo se repite:**

```bash
python3 infra/escenarios_dw.py
```

**Evidencia:** Resultado de todos los escenarios — `evidencias/escenarios_dw/RESUMEN.txt`

```text
RESUMEN DE ESCENARIOS DEL DATA WAREHOUSE

CUMPLE     Registros defectuosos  (02_registros_defectuosos.txt)
CUMPLE     Carga inicial  (01_carga_inicial.txt)
CUMPLE     Repetición de un lote exitoso  (03_repeticion_lote_exitoso.txt)
CUMPLE     Nuevo lote incremental  (04_nuevo_lote_incremental.txt)
CUMPLE     Cambio en un atributo historizado (SCD2)  (05_cambio_atributo_historizado.txt)
CUMPLE     Fallo controlado y reejecución (carga a Redshift)  (06a_fallo_carga_redshift.txt)
CUMPLE     Fallo controlado y reejecución (transformación dbt)  (06b_fallo_dbt_marts.txt)
```
