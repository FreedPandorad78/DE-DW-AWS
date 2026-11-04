#!/usr/bin/env bash
# Genera los datos de ejemplo en RDS MySQL y DocumentDB. Corre en la EC2 porque esos servicios no son públicos.
# Uso: ./infra/generar_datos_dw.sh            (semilla 42, 2 defectos por regla: los conteos de la documentación)
#      ./infra/generar_datos_dw.sh --seed 7
set -euo pipefail
cd "$(dirname "$0")/.."
source recursos_dw.env
SSH="ssh -i $HOME/.ssh/taller-dw-key.pem -o StrictHostKeyChecking=accept-new ec2-user@$EC2_IP"
$SSH "cd ~/DE-DW && docker compose --project-directory ~/DE-DW -f docker/docker-compose.yml --env-file .env run --rm --no-deps -T --entrypoint python airflow-scheduler /opt/airflow/src/ecommerce_dw/sources/generate_data.py $*"
