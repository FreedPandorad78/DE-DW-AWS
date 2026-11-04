# Decisiones técnicas y diferencias frente a la versión local

## De lo local a AWS

| Local | AWS | Por qué |
|---|---|---|
| MySQL y MongoDB en contenedores | RDS MySQL y DocumentDB | Servicios administrados, con TLS, cifrado y contraseñas en Secrets Manager |
| DuckDB (un archivo) | Redshift aprovisionado | Almacén analítico real; carga con `COPY` desde S3 |
| CSV en una carpeta local | S3, por lote y con manifiesto | Conserva los archivos para reprocesar y no sobrescribe lotes |
| `raw` se reemplazaba en cada corrida | `raw` acumula lotes (`_batch_id`) | Extracción incremental: solo llega lo nuevo |
| Extraía las tablas completas | Extracción inicial y luego incremental por marca de avance | Requisito del enunciado |
| 3 DAGs | 1 DAG de 7 pasos | Requisito del enunciado |
| Una sola EC2 de laboratorio | Misma EC2 para Airflow y dbt | El enunciado pide dbt en la misma EC2 de Airflow |

## Ajustes de SQL para Redshift

Estos ajustes salieron de la migración y de la primera corrida real. El proyecto sigue corriendo también en DuckDB (con macros por motor en `macros/compat.sql`), lo que permitió probar la lógica sin costo.

| Tema | Local (DuckDB) | Redshift | Cómo se supo |
|---|---|---|---|
| Texto a booleano | `cast(x as boolean)` | No lo permite («cannot cast type text to boolean») | Error real en la primera corrida |
| Filtro incremental de los hechos | Subconsulta correlacionada con `exists` dentro de un `or` | Se reescribió con una subconsulta `IN` no correlacionada | La corrida incremental fallaba en `marts` y pasó con el cambio; Redshift rechaza ese patrón (no se capturó el mensaje exacto) |
| Comparación segura ante nulos | `is distinct from` | La documentación de Redshift lo lista como no soportado | Documentación; macro `is_distinct_from` |
| Esquema `raw` | `raw` | `RAW` es palabra reservada: el esquema va entre comillas | Documentación (lista de palabras reservadas) |
| JSON | `json_extract_string(col, '$.a.b')` | `json_extract_path_text(col, 'a', 'b')` | Macro `json_text` |
| Hora local de Bogotá | `timezone('America/Bogota', x)` | `x at time zone 'America/Bogota'` (válido en ambos) | Macro `local_timestamp` |
| Día de la semana | `extract(isodow ...)` | `extract(dow ...)` y ajuste (válido en ambos) | Macro `iso_day_of_week` |
| Diferencia de fechas | `datediff('day', a, b)` | `a - b` entre fechas (válido en ambos) | Prueba `assert_dim_date_covers_configured_range` |

Se probó que `select * exclude (...)` sí funciona en Redshift, así que las capas `valid` no cambiaron.

## RDS MySQL frente a MySQL local

- La conexión usa TLS con el certificado de AWS (`global-bundle.pem`, incluido en la imagen) y el usuario administrador `dwadmin` en lugar de `root`; la contraseña sale de Secrets Manager.
- El esquema y los scripts de generación no cambian: `ON UPDATE CURRENT_TIMESTAMP`, restricciones `CHECK` y `SET FOREIGN_KEY_CHECKS` funcionan igual.
- No es accesible desde internet: solo desde la EC2. Para generar datos, los scripts corren dentro de la EC2.

## DocumentDB frente a MongoDB local

| Tema | MongoDB local | DocumentDB | Qué se hizo |
|---|---|---|---|
| Conexión | Texto de conexión simple | TLS obligatorio con el certificado de AWS; sin `retryWrites` | `tls=True`, `tlsCAFile`, `retryWrites=False` |
| Punto de entrada | Un servidor | Punto de entrada del clúster | `directConnection=True` (escrituras al primario) |
| Autenticación | Base `admin` | La base `admin` (y `local`) no existe como tal, pero `authSource=admin` se acepta | Se mantiene `authSource="admin"` |
| Validación de esquema | `$jsonSchema` | Compatible en la versión 5.0 de instancias; no en clústeres elásticos | Se usa DocumentDB 5.0; los validadores se conservan sin cambios |
| Documentos defectuosos de prueba | `bypass_document_validation=True` al insertar | Compatible con `insert` | La generación inyecta 34 filas defectuosas sin cambios (los datos se generaron con éxito) |
| Mensaje del cliente | — | pymongo avisa «You appear to be connected to a DocumentDB cluster» | Es informativo; no afecta el resultado |
| Costo | Gratis | Se cobra por DCU y almacenamiento | Serverless de 0,5 a 1 DCU |

## Otras decisiones

- **Redshift aprovisionado y no Serverless.** dbt ejecuta muchas sentencias pequeñas, y la documentación y el blog de AWS indican un mínimo de facturación de 60 segundos en Serverless, sin que quede claro si es por consulta o por todo el grupo de trabajo. Un clúster aprovisionado tiene un costo por hora predecible y se puede pausar.
- **`ra3.large` y no `dc2.large`.** El script prueba los tipos en orden de preferencia (`dc2.large`, `ra3.large`, `ra3.xlplus`); la cuenta no permitió `dc2.large`.
- **Un solo bucket** con prefijos (`raw/`, `quality/`, `runs/`), igual que en el data lake.
- **Contraseñas aleatorias en Secrets Manager**, nunca en el repositorio ni en archivos de la EC2; dbt las recibe por variable de entorno en cada ejecución.
- **Imagen sin buildx:** la imagen de Airflow se construye con `DOCKER_BUILDKIT=0`; el nombre de la variable de las restricciones no puede ser `PYTHON_VERSION`, porque la imagen base ya la define.
