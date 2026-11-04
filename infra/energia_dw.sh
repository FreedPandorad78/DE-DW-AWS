#!/usr/bin/env bash
# Pausa o reanuda lo que cobra por hora. Uso: ./infra/energia_dw.sh pausar | reanudar
set -uo pipefail
export AWS_PAGER=""
cd "$(dirname "$0")/.."
source recursos_dw.env
case "${1:-}" in
  pausar)
    aws redshift pause-cluster --region "$REGION" --cluster-identifier "$REDSHIFT_CLUSTER" > /dev/null && echo "Redshift: pausando"
    aws rds stop-db-instance --region "$REGION" --db-instance-identifier "$RDS_INSTANCE" > /dev/null && echo "RDS: deteniendo (AWS lo reinicia solo a los 7 días)"
    aws docdb stop-db-cluster --region "$REGION" --db-cluster-identifier "$DOCDB_CLUSTER" > /dev/null 2>&1 && echo "DocumentDB: deteniendo (AWS lo reinicia solo a los 7 días)" || echo "DocumentDB: no se pudo detener (serverless puede no permitirlo); sigue cobrando poco"
    aws ec2 stop-instances --region "$REGION" --instance-ids "$INSTANCE_ID" > /dev/null && echo "EC2: deteniendo"
    echo "Detenidos cobran solo almacenamiento. Para volver: ./infra/energia_dw.sh reanudar"
    ;;
  reanudar)
    aws redshift resume-cluster --region "$REGION" --cluster-identifier "$REDSHIFT_CLUSTER" > /dev/null && echo "Redshift: reanudando"
    aws rds start-db-instance --region "$REGION" --db-instance-identifier "$RDS_INSTANCE" > /dev/null 2>&1 && echo "RDS: iniciando"
    aws docdb start-db-cluster --region "$REGION" --db-cluster-identifier "$DOCDB_CLUSTER" > /dev/null 2>&1 && echo "DocumentDB: iniciando"
    aws ec2 start-instances --region "$REGION" --instance-ids "$INSTANCE_ID" > /dev/null && aws ec2 wait instance-running --region "$REGION" --instance-ids "$INSTANCE_ID"
    NUEVA_IP=$(aws ec2 describe-instances --region "$REGION" --instance-ids "$INSTANCE_ID" --query "Reservations[0].Instances[0].PublicIpAddress" --output text)
    sed -i '' "s|^EC2_IP=.*|EC2_IP=$NUEVA_IP|" recursos_dw.env
    echo "EC2 encendida. IP nueva: $NUEVA_IP. Redshift, RDS y DocumentDB tardan unos minutos en estar disponibles."
    ;;
  *) echo "Uso: $0 pausar | reanudar"; exit 2 ;;
esac
