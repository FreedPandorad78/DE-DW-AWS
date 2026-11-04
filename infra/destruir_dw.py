#!/usr/bin/env python3
"""Elimina TODOS los recursos del data warehouse (EC2, Redshift, RDS, DocumentDB, bucket con sus datos, secretos,
roles, grupos de seguridad y clave SSH) y verifica que no queda nada. Úsalo al terminar: borra los datos.

Uso:  python3 infra/destruir_dw.py        # pide escribir DESTRUIR
"""
import argparse
import os
import sys
import time

import boto3
from botocore.exceptions import ClientError

REGION = "us-east-2"
P = "taller-dw"


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def ya_eliminando(e):
    """True si AWS rechaza la orden porque el recurso ya está en proceso de eliminación (repetir el script es seguro)."""
    codigo = e.response["Error"]["Code"]
    mensaje = e.response["Error"].get("Message", "").lower()
    return codigo.startswith(("InvalidDBInstanceState", "InvalidClusterState", "InvalidDBClusterState")) and "delet" in mensaje


def quiet(fn, *ok_codes):
    """Ejecuta fn; si el recurso ya no existe (o el código es esperado), sigue."""
    try:
        return fn()
    except ClientError as e:
        if ya_eliminando(e):
            return None
        code = e.response["Error"]["Code"]
        if code in ok_codes or "NotFound" in code or code in ("NoSuchEntity", "NoSuchBucket", "ResourceNotFoundException"):
            return None
        raise


def wait_gone(label, probe, fast, timeout=40 * 60, interval=30):
    start = time.time()
    while True:
        if probe() is None:
            log(f"{label}: eliminado")
            return
        if time.time() - start > timeout:
            sys.exit(f"{label}: sigue existiendo después de {timeout // 60} minutos")
        log(f"{label}: eliminando...")
        time.sleep(0 if fast else interval)


def estado_docdb(docdb):
    return quiet(lambda: docdb.describe_db_clusters(DBClusterIdentifier=f"{P}-docdb")["DBClusters"][0]["Status"], "DBClusterNotFoundFault")


def encender_docdb_si_esta_detenido(docdb, fast, esperar):
    """AWS no deja eliminar un clúster de DocumentDB detenido: hay que encenderlo primero (tarda unos minutos)."""
    estado = estado_docdb(docdb)
    if estado is None:
        return
    if estado in ("stopped", "stopping") and not esperar:
        for _ in range(40):  # si todavía se está deteniendo, esperar a que termine
            if estado_docdb(docdb) == "stopped":
                break
            time.sleep(0 if fast else 15)
        log("DocumentDB está detenido: AWS exige encenderlo antes de eliminarlo; se enciende ahora")
        quiet(lambda: docdb.start_db_cluster(DBClusterIdentifier=f"{P}-docdb"))
    if esperar:
        inicio = time.time()
        while estado_docdb(docdb) not in ("available", None):
            if time.time() - inicio > 40 * 60:
                sys.exit("DocumentDB no volvió a estar disponible en 40 minutos")
            log(f"DocumentDB: {estado_docdb(docdb)}...")
            time.sleep(0 if fast else 30)


def destroy(fast=False):
    s = boto3.session.Session(region_name=REGION)
    ec2, rds, docdb, rs, iam, sm, s3 = (s.client(n) for n in ("ec2", "rds", "docdb", "redshift", "iam", "secretsmanager", "s3"))
    account = s.client("sts").get_caller_identity()["Account"]

    # 1. cómputo y bases de datos (lo que cobra por hora). DocumentDB, si está detenido, se enciende ya para eliminarlo después.
    encender_docdb_si_esta_detenido(docdb, fast, esperar=False)
    reservations = ec2.describe_instances(Filters=[{"Name": "tag:Name", "Values": [f"{P}-airflow"]}, {"Name": "instance-state-name", "Values": ["pending", "running", "stopping", "stopped"]}])["Reservations"]
    ids = [i["InstanceId"] for r in reservations for i in r["Instances"]]
    if ids:
        ec2.terminate_instances(InstanceIds=ids)
        log(f"EC2 {ids}: terminando")
    quiet(lambda: rs.delete_cluster(ClusterIdentifier=f"{P}-redshift", SkipFinalClusterSnapshot=True), "ClusterNotFound")
    quiet(lambda: rds.delete_db_instance(DBInstanceIdentifier=f"{P}-mysql", SkipFinalSnapshot=True, DeleteAutomatedBackups=True), "DBInstanceNotFound")
    encender_docdb_si_esta_detenido(docdb, fast, esperar=True)
    quiet(lambda: docdb.delete_db_instance(DBInstanceIdentifier=f"{P}-docdb-1"), "DBInstanceNotFound")
    if ids:
        quiet(lambda: ec2.get_waiter("instance_terminated").wait(InstanceIds=ids, WaiterConfig={"Delay": 1 if fast else 15, "MaxAttempts": 60}))
        log("EC2: terminada")
    wait_gone("RDS", lambda: quiet(lambda: rds.describe_db_instances(DBInstanceIdentifier=f"{P}-mysql")["DBInstances"], "DBInstanceNotFound"), fast)
    wait_gone("DocumentDB (instancia)", lambda: quiet(lambda: docdb.describe_db_instances(DBInstanceIdentifier=f"{P}-docdb-1")["DBInstances"], "DBInstanceNotFound"), fast)
    quiet(lambda: docdb.delete_db_cluster(DBClusterIdentifier=f"{P}-docdb", SkipFinalSnapshot=True), "DBClusterNotFoundFault")
    wait_gone("DocumentDB (clúster)", lambda: quiet(lambda: docdb.describe_db_clusters(DBClusterIdentifier=f"{P}-docdb")["DBClusters"], "DBClusterNotFoundFault"), fast)
    wait_gone("Redshift", lambda: quiet(lambda: rs.describe_clusters(ClusterIdentifier=f"{P}-redshift")["Clusters"], "ClusterNotFound"), fast)

    # 2. red
    quiet(lambda: rds.delete_db_subnet_group(DBSubnetGroupName=f"{P}-rds-subnets"))
    quiet(lambda: docdb.delete_db_subnet_group(DBSubnetGroupName=f"{P}-docdb-subnets"))
    quiet(lambda: rs.delete_cluster_subnet_group(ClusterSubnetGroupName=f"{P}-redshift-subnets"))
    for name in (f"{P}-datos-sg", f"{P}-ec2-sg"):
        for _ in range(20):
            found = ec2.describe_security_groups(Filters=[{"Name": "group-name", "Values": [name]}])["SecurityGroups"]
            if not found:
                break
            try:
                ec2.delete_security_group(GroupId=found[0]["GroupId"])
                log(f"grupo de seguridad {name}: eliminado")
                break
            except ClientError as e:
                if e.response["Error"]["Code"] not in ("DependencyViolation", "InvalidGroup.InUse"):
                    raise
                time.sleep(0 if fast else 10)

    # 3. clave SSH
    quiet(lambda: ec2.delete_key_pair(KeyName=f"{P}-key"))
    pem = os.path.expanduser(f"~/.ssh/{P}-key.pem")
    if os.path.exists(pem):
        os.remove(pem)
        log("clave SSH local eliminada")

    # 4. IAM
    profile = f"{P}-ec2-profile"
    quiet(lambda: iam.remove_role_from_instance_profile(InstanceProfileName=profile, RoleName=f"{P}-ec2-role"))
    quiet(lambda: iam.delete_instance_profile(InstanceProfileName=profile))
    for role in (f"{P}-ec2-role", f"{P}-redshift-role"):
        names = quiet(lambda: iam.list_role_policies(RoleName=role)["PolicyNames"]) or []
        for policy in names:
            iam.delete_role_policy(RoleName=role, PolicyName=policy)
        for attached in (quiet(lambda: iam.list_attached_role_policies(RoleName=role)["AttachedPolicies"]) or []):
            iam.detach_role_policy(RoleName=role, PolicyArn=attached["PolicyArn"])
        quiet(lambda: iam.delete_role(RoleName=role))

    # 5. secretos
    for key in ("rds", "docdb", "redshift"):
        quiet(lambda: sm.delete_secret(SecretId=f"{P}/{key}", ForceDeleteWithoutRecovery=True))

    # 6. bucket con todo su contenido
    import hashlib
    bucket = f"bucket-taller-dw-{hashlib.sha256(account.encode()).hexdigest()[:10]}"
    if quiet(lambda: s3.head_bucket(Bucket=bucket), "404", "NoSuchBucket") is not None:
        for page in s3.get_paginator("list_object_versions").paginate(Bucket=bucket):
            objs = [{"Key": o["Key"], "VersionId": o["VersionId"]} for o in page.get("Versions", []) + page.get("DeleteMarkers", [])]
            if objs:
                s3.delete_objects(Bucket=bucket, Delete={"Objects": objs})
        s3.delete_bucket(Bucket=bucket)
        log(f"bucket {bucket}: eliminado con todo su contenido")
    return bucket


def verify(bucket):
    """Devuelve (líneas, quedan): consulta cada tipo de recurso y dice si queda algo."""
    s = boto3.session.Session(region_name=REGION)
    ec2, rds, docdb, rs, iam, sm, s3 = (s.client(n) for n in ("ec2", "rds", "docdb", "redshift", "iam", "secretsmanager", "s3"))
    checks = []
    inst = [i["InstanceId"] for r in ec2.describe_instances(Filters=[{"Name": "tag:Proyecto", "Values": ["taller-dw"]}, {"Name": "instance-state-name", "Values": ["pending", "running", "stopping", "stopped", "shutting-down"]}])["Reservations"] for i in r["Instances"]]
    checks.append(("instancias EC2", inst))
    checks.append(("instancias RDS", [d["DBInstanceIdentifier"] for d in rds.describe_db_instances()["DBInstances"] if d["DBInstanceIdentifier"].startswith(P)]))
    checks.append(("clústeres de DocumentDB", [d["DBClusterIdentifier"] for d in docdb.describe_db_clusters(Filters=[{"Name": "engine", "Values": ["docdb"]}])["DBClusters"] if d["DBClusterIdentifier"].startswith(P)]))
    checks.append(("clústeres de Redshift", [d["ClusterIdentifier"] for d in rs.describe_clusters()["Clusters"] if d["ClusterIdentifier"].startswith(P)]))
    checks.append(("grupos de seguridad", [g["GroupName"] for g in ec2.describe_security_groups()["SecurityGroups"] if g["GroupName"].startswith(P)]))
    checks.append(("pares de claves", [k["KeyName"] for k in ec2.describe_key_pairs()["KeyPairs"] if k["KeyName"].startswith(P)]))
    checks.append(("roles de IAM", [r["RoleName"] for r in iam.list_roles()["Roles"] if r["RoleName"].startswith(P)]))
    checks.append(("secretos", [x["Name"] for x in sm.list_secrets()["SecretList"] if x["Name"].startswith(P)]))
    checks.append(("buckets", [b["Name"] for b in s3.list_buckets()["Buckets"] if b["Name"] == bucket]))
    lines, remain = [], False
    for label, found in checks:
        if found:
            remain = True
            lines.append(f"QUEDA   {label}: {found}")
        else:
            lines.append(f"OK      sin {label} del proyecto")
    return lines, remain


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--yes", action="store_true", help="no pedir confirmación (solo pruebas)")
    ap.add_argument("--sin-esperas", action="store_true")
    ap.add_argument("--evidencia", default="evidencias/eliminacion_dw_verificada.txt")
    ap.add_argument("--env-file", default="recursos_dw.env")
    args = ap.parse_args()
    if not args.yes:
        print("Se eliminarán EC2, Redshift, RDS, DocumentDB, el bucket con TODOS sus datos, los secretos, los roles y los grupos de seguridad.")
        if input("Escribe DESTRUIR para continuar: ").strip() != "DESTRUIR":
            sys.exit("Cancelado")
    bucket = destroy(args.sin_esperas)
    lines, remain = verify(bucket)
    header = [f"VERIFICACIÓN DE ELIMINACIÓN DEL DATA WAREHOUSE  ({time.strftime('%Y-%m-%d %H:%M:%S')})", ""]
    os.makedirs(os.path.dirname(args.evidencia) or ".", exist_ok=True)
    text = "\n".join(header + lines) + "\n"
    with open(args.evidencia, "w", encoding="utf-8") as f:
        f.write(text)
    print("\n" + text)
    if os.path.exists(args.env_file):
        os.remove(args.env_file)
    sys.exit(1 if remain else 0)


if __name__ == "__main__":
    main()
