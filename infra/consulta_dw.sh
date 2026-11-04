#!/usr/bin/env bash
# Consulta Redshift desde la EC2. Uso: ./infra/consulta_dw.sh "select count(*) from marts.fct_sales"
#                                    ./infra/consulta_dw.sh --resumen
set -uo pipefail
cd "$(dirname "$0")/.."
source recursos_dw.env
SSH="ssh -i $HOME/.ssh/taller-dw-key.pem -o StrictHostKeyChecking=accept-new ec2-user@$EC2_IP"
EXEC="cd ~/DE-DW && docker compose --project-directory ~/DE-DW -f docker/docker-compose.yml --env-file .env exec -T airflow-scheduler python -m ecommerce_dw.consulta"
if [ "${1:-}" = "--resumen" ]; then
  $SSH "$EXEC --resumen" 2>&1 | grep -v -E "UserWarning|^\s+return "
else
  printf '%s' "${*:-$(cat)}" | $SSH "$EXEC" 2>&1 | grep -v -E "UserWarning|^\s+return "
fi
