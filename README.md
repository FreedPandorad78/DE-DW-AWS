# Data warehouse de comercio electrónico en AWS

Migración del data warehouse local (MySQL + MongoDB + DuckDB + dbt + Airflow en Docker) a AWS:

```
RDS MySQL ─┐                                   ┌─ raw ─ intermediate ─ quarantine ─ marts (dbt)
           ├─► extracción incremental por lote ─► S3 raw/<fuente>/<entidad>/batch_id=<id>/ ─► COPY ─► Redshift
DocumentDB ┘     (EC2: Airflow + dbt)                                                          └─ control (lotes y marcas de avance)
```

El DAG `dw_pipeline` tiene 7 pasos: validar conexiones y parámetros, extraer, escribir en S3, cargar en Redshift,
transformar con dbt, pruebas de calidad y conciliación, y registrar el resultado.

## Estructura

| Ruta | Contenido |
|---|---|
| `src/airflow/dags/dw_pipeline.py` | DAG de los 7 pasos |
| `src/ecommerce_dw/pipeline.py` | Lógica de cada paso (probable sin Airflow) |
| `src/ecommerce_dw/ingestion/` | Extracción incremental, escritura en S3, `COPY` a Redshift y tablas de control |
| `src/ecommerce_dw/aws/` | Conexiones con TLS y secretos de Secrets Manager |
| `src/ecommerce_dw/sources/` | Generación de datos, lotes y cambios para RDS y DocumentDB |
| `src/dbt/` | Modelos, snapshots SCD2, pruebas y macros; corre en Redshift (producción) y en DuckDB (pruebas locales) |
| `docker/` | Imagen de Airflow con dbt-redshift y `docker-compose` (sin secretos) |
| `tests/test_pipeline_local.py` | Prueba de punta a punta sin AWS |
| `infra/desplegar_dw.py` | Crea la infraestructura en AWS (red, bucket, secretos, roles, RDS, DocumentDB, Redshift, EC2). `--plan` muestra qué creará y su costo por hora |
| `infra/preparar_airflow_dw.sh` | Sube el código, crea el `.env` con claves aleatorias, construye la imagen y levanta Airflow en la EC2 |
| `infra/energia_dw.sh` | `pausar` / `reanudar`: lo que cobra por hora (Redshift, RDS, DocumentDB, EC2) |
| `infra/destruir_dw.py` | Elimina todo y verifica que no queda nada (guarda la evidencia) |
| `infra/politica_user_cli_dw.json` | Permisos de IAM que necesita el usuario que despliega |
| `tests/test_despliegue.py` | Prueba del despliegue y de la destrucción contra AWS simulado |
| `docs/local_original/` | Documentación de la versión local, como referencia |

## Prueba local, sin AWS

```bash
pip install dbt-core==1.9.4 dbt-duckdb==1.9.4 duckdb mongomock "moto[s3]" Faker PyMySQL pymongo boto3
python tests/test_pipeline_local.py
```

Simula RDS (SQLite), DocumentDB (mongomock), S3 (moto) y Redshift (DuckDB con la misma SQL de carga), y ejecuta los modelos
de dbt de verdad. Comprueba que el resultado coincide con la versión local: 253 ventas, 150 reseñas, 500 eventos y la misma cuarentena.

## Diferencias con la versión local

| Local | AWS |
|---|---|
| MySQL y MongoDB en contenedores | RDS MySQL y DocumentDB, con TLS y contraseñas en Secrets Manager |
| raw se reemplaza en cada corrida | raw acumula lotes (`_batch_id`); staging toma la última versión de cada llave |
| Extrae las tablas completas | Extracción inicial y luego incremental por marca de avance, con ventana de reproceso de 7 días |
| CSV en una carpeta local | S3: `raw/<fuente>/<entidad>/batch_id=<id>/` con manifiesto; nunca se sobrescribe un lote |
| DuckDB | Redshift; el esquema `raw` va entre comillas porque `RAW` es palabra reservada |
| 3 DAGs | 1 DAG de 7 pasos, con parámetros de reproceso y de fallo controlado |

## Documentación

| Documento | Contenido |
|---|---|
| [docs/arquitectura_aws.md](docs/arquitectura_aws.md) | Diagrama, componentes, capas de Redshift y el DAG de 7 pasos |
| [docs/lotes_incrementalidad.md](docs/lotes_incrementalidad.md) | Cómo se identifica un lote, cuándo avanza la marca, extracción incremental, reproceso y recuperación |
| [docs/decisiones_tecnicas.md](docs/decisiones_tecnicas.md) | Diferencias frente a lo local, ajustes de SQL para Redshift, MongoDB frente a DocumentDB |
| [docs/calidad_y_pruebas.md](docs/calidad_y_pruebas.md) | Reglas de calidad, pruebas y escenarios |
| [docs/instrucciones.md](docs/instrucciones.md) | Despliegue, ejecución, pausa y eliminación, paso a paso |
| [docs/costos_seguridad.md](docs/costos_seguridad.md) | Estimación de costos y controles de seguridad |
| [docs/guion_demostracion_dw.md](docs/guion_demostracion_dw.md) | Guion de la demostración |

Las evidencias están en `evidencias/dw/` y `evidencias/escenarios_dw/`. La versión local original se conserva en `docs/local_original/`.
