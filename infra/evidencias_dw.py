#!/usr/bin/env python3
"""Genera las evidencias del data warehouse desde el estado REAL en AWS y las guarda en evidencias/dw/:
  airflow_evidencia.txt   tareas del DAG, ejecuciones, estado de la última exitosa y registros con lote y tarea
  dbt_evidencia.txt       resultado de cada prueba de dbt en Redshift
  indicador_ventas.txt    consulta SQL que concilia un indicador agregado (ventas por mes) entre el origen válido y el modelo
  redshift_estado.txt     lotes, marcas de avance, filas por lote en raw, hechos, dimensiones y cuarentena
  s3_evidencia.txt        estructura de lotes en S3 y un manifiesto de ejemplo
  seguridad_evidencia.txt controles de red, cifrado, acceso público y permisos
Uso: python3 infra/evidencias_dw.py
"""
import json
import os
import re
import subprocess
import sys
from datetime import datetime, timezone

import boto3

RAIZ = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
os.chdir(RAIZ)
ENV = {}
for linea in open("recursos_dw.env", encoding="utf-8"):
    if "=" in linea and not linea.startswith("#"):
        k, v = linea.strip().split("=", 1)
        ENV[k] = v
REGION = ENV["REGION"]
S3 = boto3.client("s3", region_name=REGION)
SALIDA = "evidencias/dw"
os.makedirs(SALIDA, exist_ok=True)
CLAVE = os.path.expanduser("~/.ssh/taller-dw-key.pem")
DC = "cd ~/DE-DW && docker compose --project-directory ~/DE-DW -f docker/docker-compose.yml --env-file .env"
RUIDO = re.compile(r"Warning|warnings\.warn|plugins\.py|RemovedInAirflow|alembic|^\s+return MongoClient|UserWarning")


def enmascarar(texto):
    return re.sub(r"\b\d{12}\b", "************", texto)


def ssh(comando, entrada=None, tiempo=1800):
    p = subprocess.run(["ssh", "-i", CLAVE, "-o", "StrictHostKeyChecking=accept-new", "-o", "ConnectTimeout=20", f"ec2-user@{ENV['EC2_IP']}", comando],
                       input=entrada, capture_output=True, text=True, timeout=tiempo)
    return p.stdout + p.stderr


def en_contenedor(comando, entrada=None):
    return ssh(f"{DC} exec -T airflow-scheduler {comando}", entrada)


def limpio(texto):
    return "\n".join(l for l in texto.splitlines() if not RUIDO.search(l))


def ultimo_json(texto):
    for linea in reversed(texto.splitlines()):
        if linea.strip().startswith("{"):
            try:
                return json.loads(linea)
            except ValueError:
                continue
    raise RuntimeError("Sin JSON en la respuesta:\n" + texto[-600:])


def sql(consulta):
    return ultimo_json(en_contenedor("python -m ecommerce_dw.consulta --json", consulta))["rows"]


def tabla(columnas, filas, maximo=40):
    filas = [["" if v is None else str(v)[:60] for v in f] for f in filas[:maximo]]
    ancho = [max(len(c), *(len(f[i]) for f in filas)) if filas else len(c) for i, c in enumerate(columnas)]
    fmt = "  " + " | ".join(f"{{:<{a}}}" for a in ancho)
    return "\n".join([fmt.format(*columnas), "  " + "-+-".join("-" * a for a in ancho)] + [fmt.format(*f) for f in filas]) if filas else "  (sin filas)"


def escribir(nombre, lineas):
    cabecera = f"{nombre}\nGenerado desde el estado real en AWS el {datetime.now(timezone.utc):%Y-%m-%d %H:%M:%S} UTC\n"
    open(os.path.join(SALIDA, nombre), "w", encoding="utf-8").write(enmascarar(cabecera + "\n" + "\n".join(lineas)) + "\n")
    print("escrito", os.path.join(SALIDA, nombre))


def estados_de_ejecuciones():
    resultado = []
    for pagina in S3.get_paginator("list_objects_v2").paginate(Bucket=ENV["BUCKET"], Prefix="runs/"):
        for o in pagina.get("Contents", []):
            if o["Key"].endswith("pipeline_status.json"):
                resultado.append((o["LastModified"], o["Key"]))
    return sorted(resultado, reverse=True)


# ------------------------------------------------------------------------------------------------ Airflow
def airflow():
    L = ["== 1. Tareas del DAG y sus dependencias ==", limpio(en_contenedor("airflow tasks list dw_pipeline --tree")), "",
         "== 2. Últimas ejecuciones del DAG ==", "\n".join(limpio(en_contenedor("airflow dags list-runs -d dw_pipeline")).splitlines()[:16]), ""]
    for _, clave in estados_de_ejecuciones():
        estado = json.loads(S3.get_object(Bucket=ENV["BUCKET"], Key=clave)["Body"].read())
        if estado["estado"] == "EXITOSO" and estado["mode"] == "NUEVA_EXTRACCION":
            break
    else:
        estado = None
    if estado:
        rid = estado["airflow_run_id"]
        L += [f"== 3. Última ejecución exitosa: {rid} (lote {estado['batch_id']}) ==", f"estado: {estado['estado']}   modo: {estado['mode']}   finalizó: {estado['finalizado_utc']}",
              "tareas: " + json.dumps(estado["tareas"], ensure_ascii=False, indent=2),
              "filas cargadas en raw: " + json.dumps((estado.get("resultados") or {}).get("4_cargar_redshift"), ensure_ascii=False),
              "pruebas y conciliación: " + json.dumps((estado.get("resultados") or {}).get("6_pruebas_calidad"), ensure_ascii=False), "",
              "== 4. Registros con lote y tarea (primeras líneas de cada paso) =="]
        for tarea in ("1_validar_conexiones_y_parametros", "2_extraer_mysql", "2_extraer_mongodb", "3_escribir_s3", "4_cargar_redshift", "5_dbt_marts", "6_pruebas_calidad"):
            L.append(f"--- {tarea}")
            L.append(limpio(en_contenedor(f"bash -c \"grep -h 'lote=' /opt/airflow/logs/dag_id=dw_pipeline/run_id={rid}/task_id={tarea}/attempt=*.log | sed -E 's/^\\[[^]]*\\] \\{{[^}}]*\\}} //' | cut -c1-230 | head -4\"")))
    else:
        L.append("No se encontró una ejecución exitosa de nueva extracción.")
    escribir("airflow_evidencia.txt", L)


# ------------------------------------------------------------------------------------------------ dbt
def dbt():
    salida = limpio(en_contenedor("python -m ecommerce_dw.transformations.dbt_runner test"))
    lineas = [l.replace("dbt| ", "", 1) for l in salida.splitlines() if l.startswith("dbt|") or l.startswith("{")]
    resumen = [l for l in lineas if "Done. PASS" in l]
    escribir("dbt_evidencia.txt", ["Resultado de `dbt test` en Redshift (pruebas de unicidad, campos obligatorios, relaciones, valores aceptados y reglas de negocio):", "",
                                   *(resumen or ["(sin resumen: revisar la salida)"]), "", *lineas])


# ------------------------------------------------------------------------------------------------ indicador
SQL_MODELO = """select cal.year as anio, cal.month as mes, count(*) as lineas, sum(f.subtotal) as ventas
from marts.fct_sales f join marts.dim_date cal on cal.date_sk = f.date_sk
group by cal.year, cal.month order by 1, 2"""
SQL_ORIGEN = """select cast(extract(year from o.order_date) as integer) as anio, cast(extract(month from o.order_date) as integer) as mes,
       count(*) as lineas, sum(i.subtotal) as ventas
from intermediate.int_mysql__order_items__valid i join intermediate.int_mysql__orders__valid o on o.order_id = i.order_id
group by 1, 2 order by 1, 2"""
SQL_CONCILIACION = f"""select count(*) as meses_con_diferencia from (
    select coalesce(m.anio, s.anio) as anio, coalesce(m.mes, s.mes) as mes
    from ({SQL_MODELO.replace(' order by 1, 2', '')}) m
    full outer join ({SQL_ORIGEN.replace(' order by 1, 2', '')}) s on m.anio = s.anio and m.mes = s.mes
    where m.ventas is null or s.ventas is null or m.ventas <> s.ventas or m.lineas <> s.lineas) d"""


def indicador():
    modelo, origen = sql(SQL_MODELO), sql(SQL_ORIGEN)
    dif = sql(SQL_CONCILIACION)[0][0]
    L = ["Indicador: ventas (subtotal) y líneas de pedido por mes.", "Se concilia entre el origen válido (capa intermediate, ya sin los registros en cuarentena) y el modelo analítico (marts).", "",
         "== Consulta sobre el modelo analítico (marts.fct_sales + marts.dim_date) ==", SQL_MODELO, "", tabla(["año", "mes", "líneas", "ventas"], modelo), "",
         "== Consulta sobre el origen válido (intermediate) ==", SQL_ORIGEN, "", tabla(["año", "mes", "líneas", "ventas"], origen), "",
         "== Conciliación: meses cuya venta o cantidad de líneas difiere entre el origen y el modelo ==", SQL_CONCILIACION, "", f"  resultado: {dif}",
         f"  total modelo: {sum(float(r[3]) for r in modelo):,.2f} en {sum(r[2] for r in modelo)} líneas | total origen válido: {sum(float(r[3]) for r in origen):,.2f} en {sum(r[2] for r in origen)} líneas",
         f"  CONCLUSIÓN: {'el indicador coincide en todos los meses' if dif == 0 else 'HAY DIFERENCIAS: revisar'}"]
    escribir("indicador_ventas.txt", L)


# ------------------------------------------------------------------------------------------------ estado de Redshift
def estado_redshift():
    escribir("redshift_estado.txt", [limpio(en_contenedor("python -m ecommerce_dw.consulta --resumen"))])


# ------------------------------------------------------------------------------------------------ S3
def s3():
    claves = []
    for pagina in S3.get_paginator("list_objects_v2").paginate(Bucket=ENV["BUCKET"], Prefix="raw/"):
        claves += [o["Key"] for o in pagina.get("Contents", [])]
    entidades = {}
    for k in claves:
        m = re.match(r"raw/(mysql|mongodb)/([^/]+)/batch_id=([^/]+)/(data\.csv|manifest\.json)$", k)
        if m:
            entidades.setdefault((m.group(1), m.group(2)), {}).setdefault(m.group(3), set()).add(m.group(4))
    L = [f"Bucket privado: s3://{ENV['BUCKET']}   |   estructura raw/<fuente>/<entidad>/batch_id=<id>/{{data.csv, manifest.json}}", "",
         "== Lotes por entidad (cada lote tiene su archivo y su manifiesto; ninguno se sobrescribe) =="]
    filas = []
    for (fuente, entidad), lotes in sorted(entidades.items()):
        completos = sum(1 for a in lotes.values() if a == {"data.csv", "manifest.json"})
        filas.append([fuente, entidad, len(lotes), completos, sorted(lotes)[0], sorted(lotes)[-1]])
    L += [tabla(["fuente", "entidad", "lotes", "completos", "primer lote", "último lote"], filas), ""]
    ultimo = sorted(entidades[("mysql", "orders")])[-1]
    manifiesto = json.loads(S3.get_object(Bucket=ENV["BUCKET"], Key=f"raw/mysql/orders/batch_id={ultimo}/manifest.json")["Body"].read())
    L += [f"== Manifiesto de ejemplo: raw/mysql/orders/batch_id={ultimo}/manifest.json ==",
          json.dumps({k: v for k, v in manifiesto.items() if k not in ("columns",)}, ensure_ascii=False, indent=2), "",
          "== Otros prefijos del bucket =="]
    for prefijo in ("raw/_batches/", "quality/", "runs/"):
        total = sum(len(p.get("Contents", [])) for p in S3.get_paginator("list_objects_v2").paginate(Bucket=ENV["BUCKET"], Prefix=prefijo))
        L.append(f"  {prefijo:16} {total} objetos")
    escribir("s3_evidencia.txt", L)


# ------------------------------------------------------------------------------------------------ seguridad
def seguridad():
    ec2, rds, docdb, rs, iam, sm = (boto3.client(n, region_name=REGION) for n in ("ec2", "rds", "docdb", "redshift", "iam", "secretsmanager"))
    L = ["== Red: grupos de seguridad (solo la EC2 alcanza las bases de datos) =="]
    for g in ec2.describe_security_groups(Filters=[{"Name": "group-name", "Values": ["taller-dw-*"]}])["SecurityGroups"]:
        L.append(f"  {g['GroupName']}:")
        for p in g["IpPermissions"]:
            origenes = [r["CidrIp"] for r in p.get("IpRanges", [])] + [f"grupo {x['GroupId']}" for x in p.get("UserIdGroupPairs", [])]
            L.append(f"     entra tcp/{p.get('FromPort')} desde {', '.join(origenes)}")
    d = rds.describe_db_instances(DBInstanceIdentifier=ENV["RDS_INSTANCE"])["DBInstances"][0]
    L += ["", "== RDS MySQL ==", f"  {d['DBInstanceClass']}, {d['Engine']} {d['EngineVersion']}, acceso público: {d['PubliclyAccessible']}, cifrado: {d['StorageEncrypted']}, Multi-AZ: {d['MultiAZ']}"]
    c = docdb.describe_db_clusters(DBClusterIdentifier=ENV["DOCDB_CLUSTER"])["DBClusters"][0]
    L += ["", "== DocumentDB ==", f"  motor {c['Engine']} {c['EngineVersion']}, cifrado: {c['StorageEncrypted']}, protección contra borrado: {c['DeletionProtection']}, escalado: {c.get('ServerlessV2ScalingConfiguration')}"]
    r = rs.describe_clusters(ClusterIdentifier=ENV["REDSHIFT_CLUSTER"])["Clusters"][0]
    L += ["", "== Redshift ==", f"  {r['NodeType']} x {r['NumberOfNodes']}, estado: {r['ClusterStatus']}, acceso público: {r['PubliclyAccessible']}, cifrado: {r['Encrypted']}",
          "  roles de IAM asociados: " + ", ".join(x["IamRoleArn"].split("/")[-1] for x in r.get("IamRoles", []))]
    i = ec2.describe_instances(InstanceIds=[ENV["INSTANCE_ID"]])["Reservations"][0]["Instances"][0]
    L += ["", "== EC2 de Airflow ==", f"  {i['InstanceType']}, metadatos IMDSv2 obligatorio: {i['MetadataOptions']['HttpTokens'] == 'required'}, límite de saltos: {i['MetadataOptions']['HttpPutResponseHopLimit']}, perfil: {i['IamInstanceProfile']['Arn'].split('/')[-1]}"]
    vols = ec2.describe_volumes(Filters=[{"Name": "attachment.instance-id", "Values": [ENV["INSTANCE_ID"]]}])["Volumes"]
    L.append("  discos: " + ", ".join(f"{v['Size']} GB {v['VolumeType']} cifrado={v['Encrypted']}" for v in vols))
    pab = S3.get_public_access_block(Bucket=ENV["BUCKET"])["PublicAccessBlockConfiguration"]
    enc = S3.get_bucket_encryption(Bucket=ENV["BUCKET"])["ServerSideEncryptionConfiguration"]["Rules"][0]["ApplyServerSideEncryptionByDefault"]["SSEAlgorithm"]
    L += ["", f"== Bucket s3://{ENV['BUCKET']} ==", f"  bloqueo de acceso público: {pab}", f"  cifrado por defecto: {enc}", "", "== Roles de IAM =="]
    for rol in ("taller-dw-ec2-role", "taller-dw-redshift-role"):
        politicas = iam.list_role_policies(RoleName=rol)["PolicyNames"]
        L.append(f"  {rol}: políticas en línea {politicas}")
        for nombre in politicas:
            try:
                doc = iam.get_role_policy(RoleName=rol, PolicyName=nombre)["PolicyDocument"]
                for s in doc["Statement"]:
                    L.append(f"     {s.get('Sid', '-'):22} {s['Action'] if isinstance(s['Action'], str) else ', '.join(s['Action'])}  sobre  {s['Resource']}")
            except Exception:
                L.append("     (el usuario que despliega no puede leer el contenido de la política; está en infra/desplegar_dw.py)")
    nombres = sorted(x["Name"] for x in sm.list_secrets()["SecretList"] if x["Name"].startswith("taller-dw/"))
    L += ["", "== Secretos (solo nombres; los valores no se leen ni se guardan fuera de Secrets Manager) ==", "  " + ", ".join(nombres), "", "== Archivos del repositorio: búsqueda de credenciales =="]
    hallazgos = []
    for carpeta in ("src", "infra", "docker", "docs", "tests"):
        for base, _, archivos in os.walk(carpeta):
            for a in archivos:
                ruta = os.path.join(base, a)
                try:
                    texto = open(ruta, encoding="utf-8").read()
                except (UnicodeDecodeError, OSError):
                    continue
                if re.search(r"AKIA[0-9A-Z]{16}|BEGIN [A-Z ]*PRIVATE KEY|aws_secret_access_key", texto) and not ruta.endswith(("evidencias_dw.py", "revisar_entregables.sh")):
                    hallazgos.append(ruta)
    L.append("  " + ("llaves de acceso o claves privadas encontradas en: " + ", ".join(hallazgos) if hallazgos else "ninguna llave de acceso ni clave privada en el código, la infraestructura, los docs ni las pruebas"))
    L.append(f"  docker/.env.example solo trae marcadores: {'<se genera' in open('docker/.env.example', encoding='utf-8').read()}")
    escribir("seguridad_evidencia.txt", L)


PASOS = {"airflow": airflow, "dbt": dbt, "indicador": indicador, "redshift": estado_redshift, "s3": s3, "seguridad": seguridad}

if __name__ == "__main__":
    elegidos = sys.argv[1:] or list(PASOS)
    for nombre in elegidos:
        if nombre not in PASOS:
            sys.exit(f"Desconocido: {nombre}. Opciones: {', '.join(PASOS)}")
    for nombre in elegidos:
        try:
            PASOS[nombre]()
        except Exception as error:
            print(f"ERROR en {nombre}: {error}")
