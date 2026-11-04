#!/usr/bin/env bash
# Ejecuta dbt contra Redshift desde la EC2, sin pasar por el DAG: sirve para iterar rápido y ver TODOS los errores.
# Uso: ./infra/dbt_dw.sh todo                      seed, intermediate, quarantine, snapshot, marts y test (sigue aunque algo falle)
#      ./infra/dbt_dw.sh run path:models/marts     un comando suelto
set -uo pipefail
cd "$(dirname "$0")/.."
source recursos_dw.env
SSH="ssh -i $HOME/.ssh/taller-dw-key.pem -o StrictHostKeyChecking=accept-new ec2-user@$EC2_IP"
EXEC="cd ~/DE-DW && docker compose --project-directory ~/DE-DW -f docker/docker-compose.yml --env-file .env exec -T airflow-scheduler python -m ecommerce_dw.transformations.dbt_runner"
FILTRO='Done\. PASS|Database Error|Compilation Error|Runtime Error|ERROR creating|ERROR in|Failure in test|Got [0-9]+ result|cannot |does not exist|syntax error|FALLÓ|^dbt\|   [a-z]'
if [ "${1:-}" = "todo" ]; then
  for paso in "seed" "run path:models/intermediate" "run path:models/quarantine" "snapshot" "run path:models/marts" "test"; do
    echo "=== dbt $paso"
    $SSH "$EXEC $paso" 2>&1 | grep -E "$FILTRO" | sed -E 's/^\[[^]]*\] \{[^}]*\} INFO - \[lote=[^]]*\] //' | cut -c1-200 | head -60
  done
else
  $SSH "$EXEC $*" 2>&1 | sed -E 's/^\[[^]]*\] \{[^}]*\} INFO - \[lote=[^]]*\] //' | cut -c1-220
fi
