#!/usr/bin/env bash
# Revisa que cada entregable del data warehouse exista y que no haya datos sensibles.
cd "$(dirname "$0")/.."
ok=0; falta=0
chk() { if [ -e "$1" ]; then printf "  OK     %s\n" "$1"; ok=$((ok+1)); else printf "  FALTA  %s   <- %s\n" "$1" "$2"; falta=$((falta+1)); fi; }
echo "== 1. Repositorio de código =="
chk src/airflow/dags/dw_pipeline.py "DAG de los 7 pasos"
chk src/ecommerce_dw/pipeline.py "lógica de cada paso"
chk src/ecommerce_dw/ingestion/extract.py "extracción inicial e incremental"
chk src/ecommerce_dw/ingestion/s3_batches.py "escritura de lotes en S3"
chk src/ecommerce_dw/ingestion/load_redshift.py "carga con COPY"
chk src/ecommerce_dw/ingestion/control.py "control de lotes y marcas de avance"
chk src/dbt/dbt_project.yml "proyecto dbt adaptado a Redshift"
chk src/dbt/profiles.yml "perfil de Redshift (sin secretos)"
chk src/dbt/macros/compat.sql "adaptación entre motores"
chk docker/Dockerfile "imagen de Airflow con dbt-redshift"
chk docker/.env.example "variables de ejemplo sin secretos"
chk infra/desplegar_dw.py "despliegue de la infraestructura"
chk infra/destruir_dw.py "eliminación y verificación"
chk infra/politica_user_cli_dw.json "permisos del usuario que despliega"
echo; echo "== 2. Documentación =="
chk docs/diagrama_arquitectura_dw.png "diagrama de arquitectura"
chk docs/arquitectura_aws.md "arquitectura"
chk docs/instrucciones.md "despliegue, ejecución y eliminación"
chk docs/decisiones_tecnicas.md "decisiones y diferencias frente a lo local (incluye MongoDB frente a DocumentDB)"
chk docs/lotes_incrementalidad.md "identificación de lotes y marca de avance"
chk docs/calidad_y_pruebas.md "reglas y pruebas"
chk docs/costos_seguridad.md "costos y controles de seguridad"
chk docs/guion_demostracion_dw.md "guion de la demostración"
echo; echo "== 3. Evidencias =="
for f in airflow_evidencia dbt_evidencia indicador_ventas redshift_estado s3_evidencia seguridad_evidencia; do chk evidencias/dw/$f.txt "evidencia"; done
for f in 01_carga_inicial 02_registros_defectuosos 03_repeticion_lote_exitoso 04_nuevo_lote_incremental 05_cambio_atributo_historizado 06a_fallo_carga_redshift 06b_fallo_dbt_marts RESUMEN; do chk evidencias/escenarios_dw/$f.txt "escenario"; done
chk evidencias/eliminacion_dw_verificada.txt "se genera al destruir (último paso)"
echo; echo "== 4. Resultado de cada escenario =="
for f in evidencias/escenarios_dw/0*.txt; do [ -e "$f" ] && printf "  %-10s %s\n" "$(grep -m1 '^ESTADO:' "$f" | cut -d' ' -f2-)" "$(grep -m1 '^ESCENARIO:' "$f" | cut -d' ' -f2-)"; done
echo; echo "== 5. Seguridad del repositorio =="
sens=$(grep -rnE --exclude=revisar_entregables_dw.sh --exclude=revisar_entregables.sh --exclude=evidencias_dw.py "\b[0-9]{12}\b|AKIA[0-9A-Z]{16}|aws_secret_access_key|BEGIN [A-Z ]*PRIVATE KEY" evidencias docs infra src docker tests 2>/dev/null | head -5)
if [ -z "$sens" ]; then echo "  OK     sin ID de cuenta, claves ni llaves privadas en los archivos"; else echo "  REVISAR (sin pegar los valores aquí):"; echo "$sens" | cut -d: -f1,2 | sed 's/^/     /'; fi
[ -e recursos_dw.env ] && echo "  AVISO  recursos_dw.env existe (no debe subirse a git: sin contraseñas, pero con datos de la cuenta)"
echo; echo "Resumen: $ok presentes, $falta faltan."
