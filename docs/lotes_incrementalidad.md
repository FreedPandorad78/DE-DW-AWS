# Lotes, incrementalidad y recuperación

## Cómo se identifica un lote procesado

- Cada lote tiene un identificador ordenable, `AAAAMMDDTHHMMSSZ` (UTC). Se toma de la hora de inicio de la ejecución de Airflow, que no cambia entre reintentos: **un reintento reutiliza el mismo lote**.
- El lote queda registrado en `control.batches` con su estado, y sus entidades en `control.batch_entities` (filas extraídas, filas cargadas, desde qué valor se extrajo, hasta cuál, hash del archivo).
- Cada fila de `raw` lleva su `_batch_id`, y la cuarentena conserva ese lote como **lote de origen** del rechazo.

Estados de un lote: `INICIADO` → `EN_S3` → `CARGADO` → `TRANSFORMADO` → `COMPLETO` (o `FALLIDO`).

## Cuándo se actualiza la marca de avance

La marca de avance de cada entidad (`control.watermarks`) se mueve **solo cuando el lote termina `COMPLETO`**, es decir, después de las pruebas de calidad y la conciliación. Solo avanza hacia adelante: reprocesar un lote antiguo no la retrocede. Si algo falla antes, el siguiente lote vuelve a pedir la misma ventana y no se pierde nada.

## Extracción inicial e incremental

| Entidad | Ventana de extracción |
|---|---|
| `customers`, `products` | `updated_at` **mayor** que la marca (estricto, porque el generador deja muchas filas con el mismo `updated_at`) |
| `orders`, `reviews`, `navigation_events` | Fecha del registro **mayor o igual** que la marca menos 7 días (absorbe datos tardíos) |
| `order_items` | Las líneas de los pedidos de la ventana de `orders` |

Sin marca de avance (la primera vez, o si ningún lote ha terminado) se extrae todo: es la carga inicial. Un registro puede llegar en más de un lote; `raw` los acumula y la capa de staging toma la última versión de cada llave.

## Almacenamiento en S3 sin sobrescribir

Cada entidad de cada lote va a `raw/<fuente>/<entidad>/batch_id=<id>/data.csv`, con su `manifest.json` (filas, fecha de extracción, ventana, hash SHA-256). El manifiesto se escribe al final: es la marca de que el lote está completo. Si el lote ya existe con el mismo contenido (un reintento), se reutiliza; si existe con otro contenido, se detiene en vez de sobrescribir. Los archivos se conservan para reprocesar un lote sin consultar las fuentes.

Formato: **CSV**, porque `COPY` lo lee de forma nativa, es auditable a simple vista y no exige librerías adicionales en la EC2; con estas cantidades Parquet no aporta.

## Carga a Redshift

Para cada entidad, en una transacción: tabla temporal → `COPY` → comparar el conteo con el manifiesto → borrar del `raw` las filas de ese lote → insertar con `_batch_id` → verificar. Repetir un lote **reemplaza** sus filas, no las duplica, y un fallo no deja filas a medias.

## Reprocesamiento y recuperación

- **Reproceso:** `reprocess_batch_id = <lote>` salta la extracción, usa los archivos que ya están en S3 y vuelve a cargar y transformar. El resultado es el mismo que la primera vez.
- **Fallo:** Airflow reintenta (máximo 2). Como el lote es el mismo, la carga reemplaza lo ya cargado y dbt reconstruye desde `raw`. Si algo no se recupera, el lote queda `FALLIDO`, la ejecución falla y la marca de avance no se movió.
- **Cómo se identifica una ejecución fallida:** la interfaz de Airflow (tarea en rojo), `runs/dag_run=<ejecución>/pipeline_status.json` en S3 (estado de cada tarea), `control.batches` (`FALLIDO` y detalle) y los registros con lote y tarea.

## Hechos incrementales

Los hechos son incrementales (`delete+insert` por llave). En cada ejecución se recalculan las llaves nuevas, las de la ventana de 7 días y las cuya dimensión cambió (por ejemplo, una versión SCD2 nueva). Un paso posterior (`post_hook`) elimina los hechos cuya llave de origen ya no está en la capa válida.

## Limitaciones conocidas

- Los **borrados duros** en las fuentes no se propagan (no hay marca de borrado).
- Un dato tardío más antiguo que la ventana de 7 días no se recoge.
- Dos cambios de una misma fila de cliente o producto con exactamente el mismo `updated_at` que la marca no se distinguen. En MySQL real `updated_at` lo pone el servidor y crece.
