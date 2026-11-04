#!/usr/bin/env bash
# Prepara Airflow con dbt en la EC2: sube el código, crea el .env con claves aleatorias (sin contraseñas de bases de datos:
# esas se leen de Secrets Manager), construye la imagen y levanta los contenedores.
set -euo pipefail
cd "$(dirname "$0")/.."
source recursos_dw.env
KEY="$HOME/.ssh/taller-dw-key.pem"
SSH="ssh -i $KEY -o StrictHostKeyChecking=accept-new ec2-user@$EC2_IP"

echo "Esperando a que la EC2 termine de instalar Docker..."
for i in $(seq 1 60); do
  $SSH "test -f /var/log/dw-bootstrap-ok && docker compose version > /dev/null 2>&1" 2> /dev/null && break
  sleep 10
done
$SSH "docker compose version" > /dev/null || { echo "Docker no quedó listo: revisa /var/log/cloud-init-output.log en la EC2"; exit 1; }

./infra/sync_ec2_dw.sh

$SSH bash -s << EOF
set -euo pipefail
cd ~/DE-DW
mkdir -p extracts && chmod 777 extracts
if [ ! -f .env ]; then
  rnd() { python3 -c "import secrets,string;print(''.join(secrets.choice(string.ascii_letters+string.digits) for _ in range(24)))"; }
  fernet() { python3 -c "import base64,os;print(base64.urlsafe_b64encode(os.urandom(32)).decode())"; }
  cat > .env << ENV
AIRFLOW_UID=1000
AIRFLOW_UI_PORT=8080
AIRFLOW_ADMIN_USERNAME=admin
AIRFLOW_ADMIN_PASSWORD=\$(rnd)
AIRFLOW_FERNET_KEY=\$(fernet)
AIRFLOW_SECRET_KEY=\$(rnd)
POSTGRES_USER=airflow
POSTGRES_PASSWORD=\$(rnd)
AWS_REGION=$REGION
S3_BUCKET=$BUCKET
RDS_HOST=$RDS_HOST
RDS_SECRET_ARN=$RDS_SECRET_ARN
MYSQL_DATABASE=ecommerce
DOCDB_HOST=$DOCDB_HOST
DOCDB_SECRET_ARN=$DOCDB_SECRET_ARN
MONGO_DATABASE=ecommerce
REDSHIFT_HOST=$REDSHIFT_HOST
REDSHIFT_SECRET_ARN=$REDSHIFT_SECRET_ARN
REDSHIFT_IAM_ROLE_ARN=$REDSHIFT_IAM_ROLE_ARN
REDSHIFT_DATABASE=dev
DAG_SCHEDULE=
ENV
  chmod 600 .env
  echo ".env creado con claves aleatorias"
else
  echo ".env ya existía: se conserva"
fi
echo "Construyendo la imagen (tarda varios minutos)..."
DOCKER_BUILDKIT=0 docker build -t local/ecommerce-airflow:2.11.2 docker
docker compose --project-directory ~/DE-DW -f docker/docker-compose.yml --env-file .env up -d
EOF
echo "Esperando a que Airflow registre el DAG..."
for i in $(seq 1 60); do
  out=$($SSH "cd ~/DE-DW && docker compose --project-directory ~/DE-DW -f docker/docker-compose.yml --env-file .env exec -T airflow-scheduler airflow dags list 2> /dev/null | grep dw_pipeline" || true)
  [ -n "$out" ] && break
  sleep 10
done
echo "$out"
echo "Túnel para la interfaz:  ssh -i $KEY -N -L 8080:127.0.0.1:8080 ec2-user@$EC2_IP   y luego http://localhost:8080"
echo "Usuario: admin. Contraseña: ssh -i $KEY ec2-user@$EC2_IP \"grep AIRFLOW_ADMIN_PASSWORD ~/DE-DW/.env\""
