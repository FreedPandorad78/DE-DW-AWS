#!/usr/bin/env bash
# Agrega un lote diario a las fuentes (3 pedidos, 3 reseñas y 3 eventos). Repetir el mismo número no duplica nada.
# Uso: ./infra/agregar_lote_dw.sh 40     (el número va de 1 a 365; la fecha del lote es 2026-01-01 + número - 1 días)
set -euo pipefail
cd "$(dirname "$0")/.."
source recursos_dw.env
N="${1:?uso: ./infra/agregar_lote_dw.sh <número de lote>}"
SSH="ssh -i $HOME/.ssh/taller-dw-key.pem -o StrictHostKeyChecking=accept-new ec2-user@$EC2_IP"
$SSH "cd ~/DE-DW && docker compose --project-directory ~/DE-DW -f docker/docker-compose.yml --env-file .env exec -T airflow-scheduler python /opt/airflow/src/ecommerce_dw/sources/append_batch.py --batch-id $N" 2>&1 | grep -v -E "UserWarning|^\s+return "
