#!/usr/bin/env bash
# Copia el código a la EC2 (sin secretos, sin .env, sin datos) y refresca el DAG.
set -euo pipefail
cd "$(dirname "$0")/.."
source recursos_dw.env
KEY="$HOME/.ssh/taller-dw-key.pem"
SSH="ssh -i $KEY -o StrictHostKeyChecking=accept-new ec2-user@$EC2_IP"
tar czf - --exclude='__pycache__' --exclude='.git' --exclude='.env' --exclude='recursos_dw.env' --exclude='extracts' \
  --exclude='dbt_packages' --exclude='target' --exclude='logs' src docker | $SSH "mkdir -p ~/DE-DW && tar xzf - -C ~/DE-DW"
echo "Código sincronizado con la EC2"
