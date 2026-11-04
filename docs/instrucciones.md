# Instrucciones reproducibles: despliegue, ejecución y eliminación

Los comandos se ejecutan desde la raíz del repositorio, en tu equipo, con un entorno de Python 3.12 que tenga `boto3`. Las credenciales de AWS son las del usuario `user_cli` (las que ya usa la CLI de AWS). Región: `us-east-2`.

## 0. Requisitos

1. Adjuntar al usuario que despliega la política `infra/politica_user_cli_dw.json` (en la consola de IAM: **Policies → Create policy → JSON**, y luego **Users → `user_cli` → Add permissions**). Está limitada a `us-east-2`, a los secretos `taller-dw/*`, a los roles `taller-dw-*` y a los buckets `bucket-taller-dw-*`.
2. Pruebas locales opcionales, sin AWS (la segunda necesita `moto`):
   ```bash
   pip install dbt-core==1.9.4 dbt-duckdb==1.9.4 duckdb mongomock "moto[ec2,rds,redshift,secretsmanager,iam,s3,ssm,sts]" Faker PyMySQL pymongo boto3
   python tests/test_pipeline_local.py     # pipeline completo con fuentes y S3 simulados (unos 4 minutos)
   python tests/test_despliegue.py         # despliegue y destrucción contra AWS simulado (segundos)
   ```

## 1. Desplegar la infraestructura (unos 6 a 10 minutos)

```bash
python infra/desplegar_dw.py --plan     # muestra qué se creará y el costo por hora, sin crear nada
python infra/desplegar_dw.py            # crea todo y escribe recursos_dw.env (sin contraseñas)
```

Es idempotente: si algo falla a mitad, se corrige y se repite. Si tu IP cambió, repetirlo agrega la regla de SSH para la IP nueva.

## 2. Preparar Airflow en la EC2 (unos 8 a 10 minutos)

```bash
./infra/preparar_airflow_dw.sh
```

Sube el código, crea el `.env` de la EC2 con claves aleatorias, construye la imagen y levanta Airflow. Para abrir la interfaz: `ssh -i ~/.ssh/taller-dw-key.pem -N -L 8080:127.0.0.1:8080 ec2-user@<IP>` y luego `http://localhost:8080` (usuario `admin`; la clave se lee con `grep AIRFLOW_ADMIN_PASSWORD ~/DE-DW/.env` dentro de la EC2).

## 3. Cargar datos y ejecutar el pipeline

```bash
./infra/generar_datos_dw.sh             # datos de ejemplo en RDS y DocumentDB (semilla 42, con registros defectuosos)
./infra/ejecutar_pipeline_dw.sh         # primera corrida: extracción completa; la siguiente ya es incremental
./infra/consulta_dw.sh --resumen        # lotes, marcas de avance, filas por lote, hechos, dimensiones y cuarentena
./infra/dbt_dw.sh todo                  # dbt directo contra Redshift, sin pasar por el DAG (para depurar)
```

Otras ejecuciones: `./infra/ejecutar_pipeline_dw.sh '{"reprocess_batch_id": "<lote>"}'` (reproceso desde S3) y `'{"simular_fallo": "carga_redshift"}'` (fallo controlado). Para cambiar las fuentes: `./infra/agregar_lote_dw.sh <n>` (un lote diario) y `./infra/cambiar_fuente_dw.sh --customer-id 5 --city Cali --department "Valle del Cauca" --category Deportes --changed-at 2025-09-01T00:00:00`.

## 4. Escenarios y evidencias (unos 35 a 50 minutos y 3 minutos)

```bash
python infra/escenarios_dw.py           # los seis escenarios de la sección 3.5; evidencias en evidencias/escenarios_dw/
python infra/evidencias_dw.py           # Airflow, dbt test, indicador, estado de Redshift, S3 y seguridad; en evidencias/dw/
./infra/revisar_entregables_dw.sh       # comprueba que cada entregable existe y que no hay datos sensibles
```

## 5. Pausar y reanudar (para no pagar cómputo)

```bash
./infra/energia_dw.sh pausar            # pausa Redshift; detiene RDS, DocumentDB (si lo permite) y la EC2
./infra/energia_dw.sh reanudar          # los vuelve a levantar (la IP de la EC2 cambia y se actualiza sola)
```

## 6. Eliminar todo y verificar

```bash
python infra/destruir_dw.py             # pide escribir DESTRUIR; borra EC2, Redshift, RDS, DocumentDB, bucket con sus datos, secretos, roles, grupos de seguridad y clave SSH
```

Al terminar consulta cada tipo de recurso y guarda el resultado en `evidencias/eliminacion_dw_verificada.txt`. Si queda algo, termina con error y lo lista.

## Problemas frecuentes

| Síntoma | Causa y solución |
|---|---|
| `AccessDenied` al crear roles, RDS o Redshift | Falta adjuntar `politica-dw` al usuario |
| Tiempo de espera al conectar por SSH | Cambió tu IP pública: repite `python infra/desplegar_dw.py` |
| La construcción de la imagen falla pidiendo `constraints-3.11.15.txt` | La variable no puede llamarse `PYTHON_VERSION`; el `Dockerfile` ya usa `CONSTRAINTS_PYTHON` |
| `cannot cast type text to boolean` en dbt | Redshift no convierte texto a booleano; ya corregido con la macro `to_boolean` |
