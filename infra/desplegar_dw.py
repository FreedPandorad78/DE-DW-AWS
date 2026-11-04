#!/usr/bin/env python3
"""Despliega la infraestructura del data warehouse en AWS (us-east-2): red, bucket, secretos, roles, RDS MySQL,
DocumentDB, Redshift y la EC2 de Airflow. Es idempotente: si algo ya existe, lo reutiliza.

Uso:  python3 infra/desplegar_dw.py --plan      # muestra qué creará y cuánto cuesta por hora, sin crear nada
      python3 infra/desplegar_dw.py             # crea todo y escribe recursos_dw.env

Contraseñas: se generan al azar y SOLO viven en Secrets Manager (taller-dw/rds, taller-dw/docdb, taller-dw/redshift).
Ningún servicio de datos es accesible desde internet: solo la EC2 (por grupo de seguridad) los alcanza.
"""
import argparse
import hashlib
import json
import os
import secrets
import stat
import string
import sys
import time
import urllib.request

import boto3
from botocore.exceptions import ClientError

REGION = "us-east-2"
P = "taller-dw"
DB_USER = "dwadmin"
DB_NAME = "ecommerce"
NODE_PREFERENCE = ["dc2.large", "ra3.large", "ra3.xlplus"]
# Precios de referencia por hora en us-east-2 (confirmar en la calculadora de AWS)
COST_HINT = {"EC2 t3.medium": 0.0416, "IPv4 pública": 0.005, "RDS MySQL db.t3.micro": 0.017,
             "DocumentDB serverless (0,5 DCU)": 0.041, "Redshift dc2.large (1 nodo)": 0.25}
USER_DATA = """#!/bin/bash
set -eux
dnf install -y docker git
systemctl enable --now docker
usermod -aG docker ec2-user
mkdir -p /usr/libexec/docker/cli-plugins
curl -fsSL https://github.com/docker/compose/releases/download/v2.29.7/docker-compose-linux-x86_64 -o /usr/libexec/docker/cli-plugins/docker-compose
chmod +x /usr/libexec/docker/cli-plugins/docker-compose
touch /var/log/dw-bootstrap-ok
"""


class Ctx:
    pass


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def new_password():
    alphabet = string.ascii_letters + string.digits
    return "".join(secrets.choice(alphabet) for _ in range(28))


def current_ip():
    with urllib.request.urlopen("https://checkip.amazonaws.com", timeout=10) as r:
        return r.read().decode().strip()


def tags(name):
    return [{"Key": "Name", "Value": name}, {"Key": "Proyecto", "Value": "taller-dw"}]


# ------------------------------------------------------------------------------------------------ red
def network(c):
    ec2 = c.ec2
    vpc = ec2.describe_vpcs(Filters=[{"Name": "isDefault", "Values": ["true"]}])["Vpcs"]
    if not vpc:
        sys.exit("No hay VPC por defecto en la región. Crea una con: aws ec2 create-default-vpc")
    c.vpc_id = vpc[0]["VpcId"]
    subnets = ec2.describe_subnets(Filters=[{"Name": "vpc-id", "Values": [c.vpc_id]}])["Subnets"]
    by_az = {}
    for s in subnets:
        by_az.setdefault(s["AvailabilityZone"], s["SubnetId"])
    if len(by_az) < 2:
        sys.exit("Se necesitan subredes en al menos 2 zonas de disponibilidad")
    c.subnet_ids = list(by_az.values())
    c.ec2_subnet = c.subnet_ids[0]

    def sg(name, description):
        found = ec2.describe_security_groups(Filters=[{"Name": "group-name", "Values": [name]}, {"Name": "vpc-id", "Values": [c.vpc_id]}])["SecurityGroups"]
        if found:
            return found[0]["GroupId"]
        gid = ec2.create_security_group(GroupName=name, Description=description, VpcId=c.vpc_id,
                                        TagSpecifications=[{"ResourceType": "security-group", "Tags": tags(name)}])["GroupId"]
        log(f"grupo de seguridad {name} creado")
        return gid

    c.ec2_sg = sg(f"{P}-ec2-sg", "EC2 de Airflow del data warehouse (SSH desde una IP)")
    c.data_sg = sg(f"{P}-datos-sg", "RDS, DocumentDB y Redshift: solo desde la EC2")

    def allow(group, permissions):
        try:
            ec2.authorize_security_group_ingress(GroupId=group, IpPermissions=permissions)
        except ClientError as e:
            if e.response["Error"]["Code"] != "InvalidPermission.Duplicate":
                raise

    allow(c.ec2_sg, [{"IpProtocol": "tcp", "FromPort": 22, "ToPort": 22, "IpRanges": [{"CidrIp": f"{c.ip}/32", "Description": "SSH desde mi IP"}]}])
    allow(c.data_sg, [{"IpProtocol": "tcp", "FromPort": port, "ToPort": port,
                       "UserIdGroupPairs": [{"GroupId": c.ec2_sg, "Description": desc}]}
                      for port, desc in ((3306, "RDS MySQL"), (27017, "DocumentDB"), (5439, "Redshift"))])
    log(f"red lista: VPC por defecto, {len(c.subnet_ids)} zonas, SSH solo desde {c.ip}/32")


# ------------------------------------------------------------------------------------------------ S3
def bucket(c):
    c.bucket = f"bucket-taller-dw-{hashlib.sha256(c.account.encode()).hexdigest()[:10]}"
    s3 = c.s3
    try:
        s3.head_bucket(Bucket=c.bucket)
        log(f"bucket {c.bucket} ya existe")
    except ClientError:
        s3.create_bucket(Bucket=c.bucket, CreateBucketConfiguration={"LocationConstraint": REGION})
        log(f"bucket {c.bucket} creado")
    s3.put_public_access_block(Bucket=c.bucket, PublicAccessBlockConfiguration={
        "BlockPublicAcls": True, "IgnorePublicAcls": True, "BlockPublicPolicy": True, "RestrictPublicBuckets": True})
    s3.put_bucket_encryption(Bucket=c.bucket, ServerSideEncryptionConfiguration={
        "Rules": [{"ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"}}]})


# ------------------------------------------------------------------------------------------------ IAM
def roles(c):
    iam = c.iam

    def role(name, service, policy_name, policy):
        trust = {"Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Principal": {"Service": service}, "Action": "sts:AssumeRole"}]}
        try:
            arn = iam.create_role(RoleName=name, AssumeRolePolicyDocument=json.dumps(trust), Tags=tags(name))["Role"]["Arn"]
            log(f"rol {name} creado")
        except ClientError as e:
            if e.response["Error"]["Code"] != "EntityAlreadyExists":
                raise
            arn = iam.get_role(RoleName=name)["Role"]["Arn"]
        iam.put_role_policy(RoleName=name, PolicyName=policy_name, PolicyDocument=json.dumps(policy))
        return arn

    secret_arn = f"arn:aws:secretsmanager:{REGION}:{c.account}:secret:{P}/*"
    c.ec2_role_arn = role(f"{P}-ec2-role", "ec2.amazonaws.com", "permisos-dw", {"Version": "2012-10-17", "Statement": [
        {"Sid": "S3DelBucket", "Effect": "Allow", "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"], "Resource": f"arn:aws:s3:::{c.bucket}/*"},
        {"Sid": "S3ListarBucket", "Effect": "Allow", "Action": ["s3:ListBucket", "s3:GetBucketLocation"], "Resource": f"arn:aws:s3:::{c.bucket}"},
        {"Sid": "SecretosDelProyecto", "Effect": "Allow", "Action": "secretsmanager:GetSecretValue", "Resource": secret_arn}]})
    c.redshift_role_arn = role(f"{P}-redshift-role", "redshift.amazonaws.com", "leer-s3", {"Version": "2012-10-17", "Statement": [
        {"Effect": "Allow", "Action": ["s3:GetObject"], "Resource": f"arn:aws:s3:::{c.bucket}/raw/*"},
        {"Effect": "Allow", "Action": ["s3:ListBucket", "s3:GetBucketLocation"], "Resource": f"arn:aws:s3:::{c.bucket}"}]})
    profile = f"{P}-ec2-profile"
    try:
        iam.create_instance_profile(InstanceProfileName=profile)
        iam.add_role_to_instance_profile(InstanceProfileName=profile, RoleName=f"{P}-ec2-role")
        log(f"perfil de instancia {profile} creado")
        c.iam_new = True
    except ClientError as e:
        if e.response["Error"]["Code"] != "EntityAlreadyExists":
            raise
        c.iam_new = False
    c.profile = profile


# ------------------------------------------------------------------------------------------------ secretos
def secrets_(c):
    sm = c.sm
    c.secrets = {}
    for key in ("rds", "docdb", "redshift"):
        name = f"{P}/{key}"
        try:
            value = json.loads(sm.get_secret_value(SecretId=name)["SecretString"])
            arn = sm.describe_secret(SecretId=name)["ARN"]
            log(f"secreto {name} ya existe: se reutiliza su contraseña")
        except ClientError as e:
            if e.response["Error"]["Code"] != "ResourceNotFoundException":
                raise
            value = {"username": DB_USER, "password": new_password()}
            arn = sm.create_secret(Name=name, SecretString=json.dumps(value), Tags=tags(name))["ARN"]
            log(f"secreto {name} creado (contraseña aleatoria)")
        c.secrets[key] = {"arn": arn, "value": value}


# ------------------------------------------------------------------------------------------------ bases de datos
def exists(fn, *codes):
    try:
        return fn()
    except ClientError as e:
        if e.response["Error"]["Code"] in codes:
            return None
        raise


def pick_mysql_version(client):
    """MySQL 8.4 (LTS) si la región lo ofrece; si no, 8.0. Si no se puede consultar, se usa la versión por defecto de AWS."""
    try:
        versions = sorted((v["EngineVersion"] for v in client.describe_db_engine_versions(Engine="mysql")["DBEngineVersions"]), reverse=True)
    except Exception as error:  # permiso faltante u otra causa: no es fatal
        log(f"aviso: no se pudieron consultar las versiones de MySQL ({type(error).__name__}); se usa la versión por defecto")
        return None
    return next((v for v in versions if v.startswith("8.4")), None) or next((v for v in versions if v.startswith("8.0")), None)


def rds(c):
    r = c.rds
    sub = f"{P}-rds-subnets"
    if not exists(lambda: r.describe_db_subnet_groups(DBSubnetGroupName=sub), "DBSubnetGroupNotFoundFault"):
        r.create_db_subnet_group(DBSubnetGroupName=sub, DBSubnetGroupDescription="RDS del data warehouse", SubnetIds=c.subnet_ids)
    identifier = f"{P}-mysql"
    if exists(lambda: r.describe_db_instances(DBInstanceIdentifier=identifier), "DBInstanceNotFound"):
        log(f"RDS {identifier} ya existe")
        return
    pick = pick_mysql_version(r)
    kwargs = dict(DBInstanceIdentifier=identifier, DBInstanceClass="db.t3.micro", Engine="mysql", AllocatedStorage=20, StorageType="gp3",
                  MasterUsername=DB_USER, MasterUserPassword=c.secrets["rds"]["value"]["password"], DBName=DB_NAME,
                  VpcSecurityGroupIds=[c.data_sg], DBSubnetGroupName=sub, PubliclyAccessible=False, StorageEncrypted=True,
                  BackupRetentionPeriod=0, DeletionProtection=False, MultiAZ=False, Tags=tags(identifier))
    if pick:
        kwargs["EngineVersion"] = pick
    r.create_db_instance(**kwargs)
    log(f"RDS MySQL {pick or ''} en creación (db.t3.micro, sin acceso público)")


def docdb(c):
    d = c.docdb
    sub = f"{P}-docdb-subnets"
    if not exists(lambda: d.describe_db_subnet_groups(DBSubnetGroupName=sub), "DBSubnetGroupNotFoundFault"):
        d.create_db_subnet_group(DBSubnetGroupName=sub, DBSubnetGroupDescription="DocumentDB del data warehouse", SubnetIds=c.subnet_ids)
    cluster = f"{P}-docdb"
    if not exists(lambda: d.describe_db_clusters(DBClusterIdentifier=cluster), "DBClusterNotFoundFault"):
        d.create_db_cluster(DBClusterIdentifier=cluster, Engine="docdb", EngineVersion="5.0.0", MasterUsername=DB_USER,
                            MasterUserPassword=c.secrets["docdb"]["value"]["password"], DBSubnetGroupName=sub,
                            VpcSecurityGroupIds=[c.data_sg], StorageEncrypted=True, DeletionProtection=False,
                            ServerlessV2ScalingConfiguration={"MinCapacity": 0.5, "MaxCapacity": 1.0}, Tags=tags(cluster))
        log("clúster de DocumentDB 5.0 en creación (serverless 0,5 a 1 DCU)")
    instance = f"{P}-docdb-1"
    if not exists(lambda: d.describe_db_instances(DBInstanceIdentifier=instance), "DBInstanceNotFound"):
        d.create_db_instance(DBInstanceIdentifier=instance, DBInstanceClass="db.serverless", Engine="docdb", DBClusterIdentifier=cluster, Tags=tags(instance))
        log("instancia serverless de DocumentDB en creación")


def redshift(c):
    r = c.redshift
    sub = f"{P}-redshift-subnets"
    if not exists(lambda: r.describe_cluster_subnet_groups(ClusterSubnetGroupName=sub), "ClusterSubnetGroupNotFoundFault"):
        r.create_cluster_subnet_group(ClusterSubnetGroupName=sub, Description="Redshift del data warehouse", SubnetIds=c.subnet_ids)
    cluster = f"{P}-redshift"
    if exists(lambda: r.describe_clusters(ClusterIdentifier=cluster), "ClusterNotFound"):
        log(f"Redshift {cluster} ya existe")
        return
    try:
        available = {o["NodeType"] for o in r.describe_orderable_cluster_options().get("OrderableClusterOptions", [])}
    except Exception as error:  # permiso faltante u otra causa: se prueba cada tipo en orden de preferencia
        log(f"aviso: no se pudieron consultar los tipos de nodo ({type(error).__name__}); se prueban en orden")
        available = set()
    candidates = [n for n in NODE_PREFERENCE if not available or n in available] or NODE_PREFERENCE
    for node in candidates:
        try:
            r.create_cluster(ClusterIdentifier=cluster, NodeType=node, ClusterType="single-node", MasterUsername=DB_USER,
                             MasterUserPassword=c.secrets["redshift"]["value"]["password"], DBName="dev", ClusterSubnetGroupName=sub,
                             VpcSecurityGroupIds=[c.data_sg], IamRoles=[c.redshift_role_arn], PubliclyAccessible=False, Encrypted=True,
                             Tags=tags(cluster))
            break
        except ClientError as e:
            if node != candidates[-1] and e.response["Error"]["Code"] in ("UnsupportedOptionFault", "InvalidParameterValue", "InvalidParameterCombination"):
                log(f"el tipo de nodo {node} no está disponible en esta cuenta ({e.response['Error']['Code']}); se prueba el siguiente")
                continue
            raise
    c.node_type = node
    log(f"Redshift {node} de un nodo en creación (sin acceso público)")


# ------------------------------------------------------------------------------------------------ esperas y puntos de conexión
def wait_for(label, probe, ok, c, interval=30, timeout=45 * 60):
    start = time.time()
    while True:
        status = probe()
        if status in ok:
            log(f"{label}: {status}")
            return
        if time.time() - start > timeout:
            sys.exit(f"{label}: no estuvo listo en {timeout // 60} minutos (estado: {status})")
        log(f"{label}: {status}...")
        time.sleep(0 if c.fast else interval)


def endpoints(c):
    wait_for("RDS", lambda: c.rds.describe_db_instances(DBInstanceIdentifier=f"{P}-mysql")["DBInstances"][0]["DBInstanceStatus"], {"available"}, c)
    wait_for("DocumentDB (instancia)", lambda: c.docdb.describe_db_instances(DBInstanceIdentifier=f"{P}-docdb-1")["DBInstances"][0]["DBInstanceStatus"], {"available"}, c)
    wait_for("Redshift", lambda: c.redshift.describe_clusters(ClusterIdentifier=f"{P}-redshift")["Clusters"][0]["ClusterStatus"], {"available"}, c)
    c.rds_host = c.rds.describe_db_instances(DBInstanceIdentifier=f"{P}-mysql")["DBInstances"][0]["Endpoint"]["Address"]
    c.docdb_host = c.docdb.describe_db_clusters(DBClusterIdentifier=f"{P}-docdb")["DBClusters"][0]["Endpoint"]
    c.redshift_host = c.redshift.describe_clusters(ClusterIdentifier=f"{P}-redshift")["Clusters"][0]["Endpoint"]["Address"]
    for key, host, port in (("rds", c.rds_host, 3306), ("docdb", c.docdb_host, 27017), ("redshift", c.redshift_host, 5439)):
        value = dict(c.secrets[key]["value"], host=host, port=port)
        c.sm.put_secret_value(SecretId=c.secrets[key]["arn"], SecretString=json.dumps(value))


# ------------------------------------------------------------------------------------------------ EC2
def ec2_instance(c):
    ec2 = c.ec2
    key = f"{P}-key"
    pem = os.path.expanduser(f"~/.ssh/{key}.pem")
    if not exists(lambda: ec2.describe_key_pairs(KeyNames=[key]), "InvalidKeyPair.NotFound"):
        material = ec2.create_key_pair(KeyName=key, KeyType="rsa", TagSpecifications=[{"ResourceType": "key-pair", "Tags": tags(key)}])["KeyMaterial"]
        os.makedirs(os.path.dirname(pem), exist_ok=True)
        with open(pem, "w") as f:
            f.write(material)
        os.chmod(pem, stat.S_IRUSR | stat.S_IWUSR)
        log(f"par de claves creado: {pem}")
    elif not os.path.exists(pem):
        sys.exit(f"El par de claves {key} existe en AWS pero no tienes {pem}. Bórralo con destruir_dw.py o crea otro.")
    c.key_name, c.pem = key, pem
    found = ec2.describe_instances(Filters=[{"Name": "tag:Name", "Values": [f"{P}-airflow"]}, {"Name": "instance-state-name", "Values": ["pending", "running", "stopping", "stopped"]}])
    instances = [i for r in found["Reservations"] for i in r["Instances"]]
    if instances:
        c.instance_id = instances[0]["InstanceId"]
        log(f"EC2 {c.instance_id} ya existe")
    else:
        ami = boto3.client("ssm", region_name=REGION).get_parameter(Name="/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64")["Parameter"]["Value"]
        if c.iam_new and not c.fast:
            log("esperando 15 s a que IAM propague el perfil de instancia")
            time.sleep(15)
        c.instance_id = ec2.run_instances(
            ImageId=ami, InstanceType="t3.medium", MinCount=1, MaxCount=1, KeyName=key, SubnetId=c.ec2_subnet, SecurityGroupIds=[c.ec2_sg],
            IamInstanceProfile={"Name": c.profile}, UserData=USER_DATA,
            BlockDeviceMappings=[{"DeviceName": "/dev/xvda", "Ebs": {"VolumeSize": 30, "VolumeType": "gp3", "Encrypted": True, "DeleteOnTermination": True}}],
            MetadataOptions={"HttpTokens": "required", "HttpPutResponseHopLimit": 2, "HttpEndpoint": "enabled"},
            TagSpecifications=[{"ResourceType": "instance", "Tags": tags(f"{P}-airflow")}])["Instances"][0]["InstanceId"]
        log(f"EC2 {c.instance_id} (t3.medium) en creación")
    wait_for("EC2", lambda: ec2.describe_instances(InstanceIds=[c.instance_id])["Reservations"][0]["Instances"][0]["State"]["Name"], {"running"}, c, interval=10)
    c.ec2_ip = ec2.describe_instances(InstanceIds=[c.instance_id])["Reservations"][0]["Instances"][0].get("PublicIpAddress", "")


def write_env(c, path):
    lines = {
        "REGION": REGION, "BUCKET": c.bucket, "INSTANCE_ID": c.instance_id, "EC2_IP": c.ec2_ip, "KEY_NAME": c.key_name,
        "EC2_SG_ID": c.ec2_sg, "DATA_SG_ID": c.data_sg, "RDS_HOST": c.rds_host, "DOCDB_HOST": c.docdb_host,
        "REDSHIFT_HOST": c.redshift_host, "RDS_SECRET_ARN": c.secrets["rds"]["arn"], "DOCDB_SECRET_ARN": c.secrets["docdb"]["arn"],
        "REDSHIFT_SECRET_ARN": c.secrets["redshift"]["arn"], "REDSHIFT_IAM_ROLE_ARN": c.redshift_role_arn,
        "REDSHIFT_CLUSTER": f"{P}-redshift", "RDS_INSTANCE": f"{P}-mysql", "DOCDB_CLUSTER": f"{P}-docdb",
    }
    with open(path, "w") as f:
        f.write("# Generado por infra/desplegar_dw.py. Sin contraseñas: están en Secrets Manager. No se sube a git.\n")
        for k, v in lines.items():
            f.write(f"{k}={v}\n")
    log(f"escrito {path}")


def plan():
    print("Se crearán (o reutilizarán, si ya existen) estos recursos en us-east-2:\n")
    for item in ("Grupos de seguridad: EC2 (SSH solo desde tu IP) y datos (solo desde la EC2)", "Bucket S3 privado y cifrado: bucket-taller-dw-<hash>",
                 "Roles de IAM: taller-dw-ec2-role (S3 y secretos) y taller-dw-redshift-role (leer S3)",
                 "Secretos taller-dw/rds, taller-dw/docdb y taller-dw/redshift con contraseñas aleatorias",
                 "RDS MySQL 8.4 db.t3.micro (20 GB, sin acceso público)", "DocumentDB 5.0 serverless 0,5 a 1 DCU (sin acceso público)",
                 "Redshift de un nodo (dc2.large si la cuenta lo permite; si no, ra3), sin acceso público",
                 "EC2 t3.medium con Docker, IMDSv2 obligatorio, disco gp3 de 30 GB"):
        print("  -", item)
    total = sum(COST_HINT.values())
    print("\nCosto aproximado por hora mientras TODO esté encendido (precios de referencia):")
    for k, v in COST_HINT.items():
        print(f"  {k:36} USD {v:.3f}")
    print(f"  {'Total':36} USD {total:.2f}  (≈ USD {total * 24:.0f} por día si se olvida encendido)")
    print("\nPara no gastar de más: pausa Redshift y detén la EC2, RDS y DocumentDB entre sesiones, y destruye todo al terminar.")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--plan", action="store_true", help="solo muestra el plan y el costo, sin crear nada")
    ap.add_argument("--ip", help="IP desde la que se permite SSH (por defecto, la actual)")
    ap.add_argument("--env-file", default="recursos_dw.env")
    ap.add_argument("--sin-esperas", action="store_true", help="solo para pruebas con AWS simulado")
    args = ap.parse_args()
    if args.plan:
        return plan()
    c = Ctx()
    c.fast = args.sin_esperas
    c.ip = args.ip or current_ip()
    session = boto3.session.Session(region_name=REGION)
    for name in ("ec2", "s3", "iam", "rds", "redshift"):
        setattr(c, name, session.client(name))
    c.sm, c.docdb = session.client("secretsmanager"), session.client("docdb")
    c.account = session.client("sts").get_caller_identity()["Account"]
    log(f"desplegando en la cuenta ***{c.account[-4:]} región {REGION}")
    network(c); bucket(c); roles(c); secrets_(c)
    rds(c); docdb(c); redshift(c)          # se crean en paralelo: lo lento es esperar
    endpoints(c); ec2_instance(c)
    write_env(c, args.env_file)
    log("listo. Siguiente paso: ./infra/preparar_airflow_dw.sh")


if __name__ == "__main__":
    main()
