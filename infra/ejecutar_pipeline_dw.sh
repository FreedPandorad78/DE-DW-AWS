#!/usr/bin/env bash
# Dispara el DAG dw_pipeline y espera su resultado.
# Uso: ./infra/ejecutar_pipeline_dw.sh                                          nueva extracción
#      ./infra/ejecutar_pipeline_dw.sh '{"reprocess_batch_id": "AAAAMMDDTHHMMSSZ"}'   reprocesa un lote desde S3
#      ./infra/ejecutar_pipeline_dw.sh '{"simular_fallo": "carga_redshift"}'          fallo controlado (se recupera solo)
set -uo pipefail
cd "$(dirname "$0")/.."
source recursos_dw.env
SSH="ssh -i $HOME/.ssh/taller-dw-key.pem -o StrictHostKeyChecking=accept-new ec2-user@$EC2_IP"
CONF="${1:-}"; [ -z "$CONF" ] && CONF='{}'
RID="manual_$(date +%m%d%H%M%S)"
AF="cd ~/DE-DW && docker compose --project-directory ~/DE-DW -f docker/docker-compose.yml --env-file .env exec -T airflow-scheduler airflow"
$SSH "$AF dags unpause dw_pipeline" > /dev/null 2>&1
$SSH bash -s << EOF > /dev/null 2>&1
$AF dags trigger dw_pipeline --run-id '$RID' --conf '$CONF'
EOF
echo "Ejecución $RID disparada (configuración: $CONF). Espero el resultado, hasta 30 minutos..."
for i in $(seq 1 360); do
  if aws s3api head-object --bucket "$BUCKET" --key "runs/dag_run=$RID/pipeline_status.json" > /dev/null 2>&1; then break; fi
  if [ $((i % 12)) -eq 0 ]; then
    echo "  $(date +%H:%M:%S) sigue en ejecución:"
    $SSH "$AF tasks states-for-run dw_pipeline $RID" 2> /dev/null | grep -E "^dw_pipeline|[0-9]_" | awk -F'|' '{gsub(/ /,"",$0); print "     " $0}' | cut -c1-110 | tail -14
  fi
  sleep 5
done
aws s3 cp "s3://$BUCKET/runs/dag_run=$RID/pipeline_status.json" - 2> /dev/null | python3 -c "
import sys, json
d = json.load(sys.stdin)
print('Lote:', d['batch_id'], '| modo:', d['mode'], '| estado:', d['estado'])
malas = {k: v for k, v in d['tareas'].items() if v != 'success'}
print('Tareas sin éxito:', malas or 'ninguna')
r = d.get('resultados') or {}
print('Filas cargadas en raw:', r.get('4_cargar_redshift'))
print('Pruebas y conciliación:', r.get('6_pruebas_calidad'))
" || echo "No apareció el resultado de $RID. Revisa la interfaz de Airflow o los registros."
echo "Ejecución: $RID"
