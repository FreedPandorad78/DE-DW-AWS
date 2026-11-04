#!/usr/bin/env bash
# Cambia ciudad/departamento de un cliente y la categoría de un producto en RDS (para demostrar SCD2).
# Uso: ./infra/cambiar_fuente_dw.sh --customer-id 5 --product-id 5 --city Cali --department "Valle del Cauca" --category Deportes --changed-at 2025-09-01T00:00:00
set -euo pipefail
cd "$(dirname "$0")/.."
source recursos_dw.env
SSH="ssh -i $HOME/.ssh/taller-dw-key.pem -o StrictHostKeyChecking=accept-new ec2-user@$EC2_IP"
ARGS=""; for a in "$@"; do ARGS="$ARGS '$a'"; done
$SSH "cd ~/DE-DW && docker compose --project-directory ~/DE-DW -f docker/docker-compose.yml --env-file .env exec -T airflow-scheduler python /opt/airflow/src/ecommerce_dw/sources/simulate_changes.py $ARGS" 2>&1 | grep -v -E "UserWarning|^\s+return "
