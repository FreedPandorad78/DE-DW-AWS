"""Prueba del despliegue y la destrucción contra AWS simulado (moto). No crea nada real.
  python tests/test_despliegue.py"""
import json, os, sys, tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
HOME = tempfile.mkdtemp(prefix="home_")
os.environ.update(HOME=HOME, AWS_ACCESS_KEY_ID="x", AWS_SECRET_ACCESS_KEY="x", AWS_DEFAULT_REGION="us-east-2")
sys.path.insert(0, str(ROOT / "infra"))

import boto3
from moto import mock_aws
import desplegar_dw as dep, destruir_dw as des


# moto enruta DocumentDB al backend de RDS (comparten servidor), así que DocumentDB se simula aquí, pero cada llamada se
# valida contra el modelo REAL del servicio con botocore: un parámetro mal escrito falla igual que fallaría en AWS.
from botocore.exceptions import ClientError
from botocore.validate import validate_parameters

_original_client = boto3.session.Session.client


class FakeDocDb:
    def __init__(self):
        self.real = _original_client(boto3.session.Session(region_name="us-east-2"), "docdb")
        self.subnets, self.clusters, self.instances = {}, {}, {}

    def _check(self, operation, kwargs):
        validate_parameters(kwargs, self.real.meta.service_model.operation_model(operation).input_shape)

    @staticmethod
    def _missing(code, operation):
        return ClientError({"Error": {"Code": code, "Message": "no existe"}}, operation)

    def describe_db_subnet_groups(self, **kw):
        self._check("DescribeDBSubnetGroups", kw)
        if kw["DBSubnetGroupName"] not in self.subnets: raise self._missing("DBSubnetGroupNotFoundFault", "DescribeDBSubnetGroups")
        return {"DBSubnetGroups": [self.subnets[kw["DBSubnetGroupName"]]]}

    def create_db_subnet_group(self, **kw):
        self._check("CreateDBSubnetGroup", kw); self.subnets[kw["DBSubnetGroupName"]] = kw; return {}

    def delete_db_subnet_group(self, **kw):
        self._check("DeleteDBSubnetGroup", kw); self.subnets.pop(kw["DBSubnetGroupName"], None); return {}

    def describe_db_clusters(self, **kw):
        self._check("DescribeDBClusters", kw)
        if "DBClusterIdentifier" in kw:
            if kw["DBClusterIdentifier"] not in self.clusters: raise self._missing("DBClusterNotFoundFault", "DescribeDBClusters")
            return {"DBClusters": [self.clusters[kw["DBClusterIdentifier"]]]}
        return {"DBClusters": list(self.clusters.values())}

    def create_db_cluster(self, **kw):
        self._check("CreateDBCluster", kw)
        self.clusters[kw["DBClusterIdentifier"]] = dict(kw, Endpoint=f"{kw['DBClusterIdentifier']}.cluster-x.us-east-2.docdb.amazonaws.com")
        return {}

    def delete_db_cluster(self, **kw):
        self._check("DeleteDBCluster", kw); self.clusters.pop(kw["DBClusterIdentifier"], None); return {}

    def describe_db_instances(self, **kw):
        self._check("DescribeDBInstances", kw)
        if kw["DBInstanceIdentifier"] not in self.instances: raise self._missing("DBInstanceNotFound", "DescribeDBInstances")
        return {"DBInstances": [dict(self.instances[kw["DBInstanceIdentifier"]], DBInstanceStatus="available")]}

    def create_db_instance(self, **kw):
        self._check("CreateDBInstance", kw); self.instances[kw["DBInstanceIdentifier"]] = kw; return {}

    def delete_db_instance(self, **kw):
        self._check("DeleteDBInstance", kw); self.instances.pop(kw["DBInstanceIdentifier"], None); return {}


FAKE_DOCDB = FakeDocDb()
boto3.session.Session.client = lambda self, name, *a, **k: FAKE_DOCDB if name == "docdb" else _original_client(self, name, *a, **k)


def ok(cond, msg):
    print(("  OK     " if cond else "  FALLA  ") + msg)
    if not cond:
        sys.exit(1)


@mock_aws
def main():
    env = os.path.join(HOME, "recursos_dw.env")
    sys.argv = ["desplegar_dw.py", "--ip", "203.0.113.7", "--sin-esperas", "--env-file", env]
    print("\n== Despliegue ==")
    dep.main()
    ec2, s3, iam, sm = (boto3.client(n) for n in ("ec2", "s3", "iam", "secretsmanager"))
    rds, docdb, rs = boto3.client("rds"), boto3.client("docdb"), boto3.client("redshift")
    sgs = {g["GroupName"]: g for g in ec2.describe_security_groups()["SecurityGroups"] if g["GroupName"].startswith("taller-dw")}
    ok(set(sgs) == {"taller-dw-ec2-sg", "taller-dw-datos-sg"}, "2 grupos de seguridad")
    ssh = [p for p in sgs["taller-dw-ec2-sg"]["IpPermissions"] if p.get("FromPort") == 22][0]
    ok(ssh["IpRanges"][0]["CidrIp"] == "203.0.113.7/32", "SSH solo desde la IP indicada (/32)")
    datos = sgs["taller-dw-datos-sg"]["IpPermissions"]
    ok(sorted(p["FromPort"] for p in datos) == [3306, 5439, 27017] and all(not p.get("IpRanges") for p in datos), "RDS, DocumentDB y Redshift: solo desde la EC2 (sin rangos de IP)")
    bucket = [b["Name"] for b in s3.list_buckets()["Buckets"] if b["Name"].startswith("bucket-taller-dw-")][0]
    ok(all(s3.get_public_access_block(Bucket=bucket)["PublicAccessBlockConfiguration"].values()), "bucket privado (4 bloqueos de acceso público)")
    ok(s3.get_bucket_encryption(Bucket=bucket)["ServerSideEncryptionConfiguration"]["Rules"], "bucket cifrado")
    roles = {r["RoleName"] for r in iam.list_roles()["Roles"] if r["RoleName"].startswith("taller-dw")}
    ok(roles == {"taller-dw-ec2-role", "taller-dw-redshift-role"}, "2 roles de IAM")
    policy = iam.get_role_policy(RoleName="taller-dw-ec2-role", PolicyName="permisos-dw")["PolicyDocument"]
    ok(all(s["Resource"] != "*" for s in policy["Statement"]), "el rol de la EC2 no tiene permisos sobre '*'")
    names = {s["Name"] for s in sm.list_secrets()["SecretList"]}
    ok(names == {"taller-dw/rds", "taller-dw/docdb", "taller-dw/redshift"}, "3 secretos")
    secret = json.loads(sm.get_secret_value(SecretId="taller-dw/rds")["SecretString"])
    ok(len(secret["password"]) >= 24 and secret["host"] and secret["username"] == "dwadmin", "el secreto tiene usuario, contraseña aleatoria y host")
    ok(rds.describe_db_instances(DBInstanceIdentifier="taller-dw-mysql")["DBInstances"][0]["PubliclyAccessible"] is False, "RDS sin acceso público")
    ok(docdb.describe_db_clusters(DBClusterIdentifier="taller-dw-docdb")["DBClusters"][0]["StorageEncrypted"], "DocumentDB cifrado (parámetros validados contra el modelo real de DocumentDB)")
    cl = rs.describe_clusters(ClusterIdentifier="taller-dw-redshift")["Clusters"][0]
    ok(cl["PubliclyAccessible"] is False and cl["Encrypted"] and cl["IamRoles"], "Redshift privado, cifrado y con rol para leer S3")
    inst = [i for r in ec2.describe_instances()["Reservations"] for i in r["Instances"]][0]
    ok(inst["MetadataOptions"]["HttpTokens"] == "required" and inst["MetadataOptions"]["HttpPutResponseHopLimit"] == 2, "EC2 con IMDSv2 obligatorio y límite de saltos 2")
    text = Path(env).read_text()
    ok("password" not in text.lower() and secret["password"] not in text, "recursos_dw.env no contiene contraseñas")
    ok(all(k in text for k in ("BUCKET=", "RDS_HOST=", "DOCDB_HOST=", "REDSHIFT_HOST=", "REDSHIFT_IAM_ROLE_ARN=", "EC2_IP=")), "recursos_dw.env tiene los datos de conexión")
    ok(oct(os.stat(os.path.join(HOME, ".ssh", "taller-dw-key.pem")).st_mode)[-3:] == "600", "clave SSH con permisos 600")

    print("\n== Idempotencia: desplegar de nuevo no duplica nada ==")
    before = (len([r for x in ec2.describe_instances()["Reservations"] for r in x["Instances"]]), len(sm.list_secrets()["SecretList"]))
    pw = secret["password"]
    dep.main()
    after = (len([r for x in ec2.describe_instances()["Reservations"] for r in x["Instances"]]), len(sm.list_secrets()["SecretList"]))
    ok(before == after, "la misma cantidad de instancias y de secretos")
    ok(json.loads(sm.get_secret_value(SecretId="taller-dw/rds")["SecretString"])["password"] == pw, "la contraseña no cambia al redesplegar")

    print("\n== Destrucción y verificación ==")
    s3.put_object(Bucket=bucket, Key="raw/mysql/customers/batch_id=1/data.csv", Body=b"x")
    sys.argv = ["destruir_dw.py", "--yes", "--sin-esperas", "--evidencia", os.path.join(HOME, "eliminacion.txt"), "--env-file", env]
    try:
        des.main()
    except SystemExit as e:
        ok(e.code == 0, "la destrucción termina con código 0")
    report = Path(HOME, "eliminacion.txt").read_text()
    ok("QUEDA" not in report and report.count("OK ") == 9, "la verificación no encuentra ningún recurso")
    ok(not Path(env).exists() and not Path(HOME, ".ssh", "taller-dw-key.pem").exists(), "se borran recursos_dw.env y la clave SSH")
    print("\nTODAS LAS PRUEBAS PASARON")


if __name__ == "__main__":
    main()
