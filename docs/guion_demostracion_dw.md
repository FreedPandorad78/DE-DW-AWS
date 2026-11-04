# Guion de la demostración del data warehouse (unos 20 minutos)

QuickSight no forma parte del alcance (indicación del profesor). La demostración llega hasta las tablas de `marts` y la conciliación de un indicador.

## Antes de empezar (la infraestructura está pausada)

```bash
./infra/energia_dw.sh reanudar          # esperar unos 10 minutos: Redshift y las bases tardan en volver
ssh -i ~/.ssh/taller-dw-key.pem -N -L 8080:127.0.0.1:8080 ec2-user@$(grep ^EC2_IP recursos_dw.env | cut -d= -f2)   # en OTRA terminal; luego http://localhost:8080
./infra/consulta_dw.sh --resumen        # confirma que todo responde
```

## 1. Estado antes (2 minutos)

`./infra/consulta_dw.sh --resumen`: muestra los lotes y su estado, las marcas de avance, las filas de `raw` por lote, los hechos y la cuarentena. Qué decir: cada lote tiene su identificador; la marca de avance solo avanza cuando el lote termina `COMPLETO`.

## 2. Procesar un lote nuevo desde las fuentes hasta marts (6 minutos)

```bash
./infra/agregar_lote_dw.sh 40           # 3 pedidos, 3 reseñas y 3 eventos nuevos en RDS y DocumentDB
./infra/ejecutar_pipeline_dw.sh         # mientras corre, mostrar el DAG en Airflow (vista Graph)
```

Qué mostrar: los 7 pasos en verde en orden; el registro de `2_extraer_mysql` (cuántas filas, desde qué ventana); en S3 la ruta `raw/<fuente>/<entidad>/batch_id=<id>/` con su manifiesto. Qué decir: la extracción es incremental, solo trae lo nuevo más la ventana de 7 días; los datos no se sobrescriben.

## 3. Estado después y conciliación (3 minutos)

```bash
./infra/consulta_dw.sh --resumen        # cada hecho creció exactamente en 3 filas; el lote nuevo trae pocas filas en raw
python infra/evidencias_dw.py indicador && tail -8 evidencias/dw/indicador_ventas.txt
```

Qué decir: la conciliación compara ventas y líneas por mes entre el origen válido y el modelo, y da cero diferencias.

## 4. Fallo y recuperación (7 minutos)

```bash
./infra/ejecutar_pipeline_dw.sh '{"simular_fallo": "carga_redshift"}'
```

Qué mostrar: en Airflow, la tarea `4_cargar_redshift` falla en el primer intento (registro `FALLO SIMULADO` con el lote) y se recupera en el segundo; al terminar, `./infra/consulta_dw.sh --resumen` no muestra duplicados.

**Cómo se identifica una ejecución fallida:** la tarea en rojo en Airflow; `runs/dag_run=<ejecución>/pipeline_status.json` en S3; `control.batches` con estado `FALLIDO` y detalle; los registros con `[lote=...] [tarea=...]`.
**Cómo se recupera:** Airflow reintenta con el mismo lote; la carga reemplaza las filas del lote en `raw` (sin duplicar) y dbt reconstruye; si no se recupera, la marca de avance no se movió y se reejecuta el mismo lote o se reprocesa con `reprocess_batch_id`.

## Si algo sale mal

Cada escenario y cada evidencia ya están generados en `evidencias/escenarios_dw/` y `evidencias/dw/`: se puede mostrar el archivo en lugar de volver a ejecutar.

## Al terminar la entrega

`python infra/destruir_dw.py` y guardar `evidencias/eliminacion_dw_verificada.txt`.

## Preguntas probables

- **¿Cómo evitan duplicados?** `raw` guarda el lote en su propia identidad y repetirlo reemplaza sus filas; staging toma la última versión de cada llave; los hechos se actualizan por llave.
- **¿Qué pasa con un registro defectuoso?** Va a cuarentena con su motivo y su lote de origen, y no entra a los hechos.
- **¿Cómo se historizan los cambios?** Snapshots SCD2 de clientes y productos; los hechos apuntan a la versión vigente en la fecha del evento.
- **¿Por qué Redshift aprovisionado?** Costo predecible con muchas sentencias pequeñas de dbt, y se puede pausar.
