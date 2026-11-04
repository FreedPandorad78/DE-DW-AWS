"""Conexiones a RDS MySQL, DocumentDB y Redshift. Con TLS; las contraseñas salen de Secrets Manager.

Para trabajar en local (sin AWS) siguen funcionando MYSQL_USER/MYSQL_PASSWORD y MONGODB_URI.
"""
import os

from .secrets import credentials


def connect_mysql(autocommit=False, cursorclass=None):
    import pymysql

    if os.getenv("RDS_SECRET_ARN"):
        user, password = credentials("RDS_SECRET_ARN")
    else:
        user, password = os.environ["MYSQL_USER"], os.environ["MYSQL_PASSWORD"]
    options = {"ssl": {"ca": os.environ["RDS_CA_BUNDLE"]}} if os.getenv("RDS_CA_BUNDLE") else {}
    if cursorclass is not None:
        options["cursorclass"] = cursorclass
    return pymysql.connect(
        host=os.getenv("MYSQL_HOST", os.getenv("RDS_HOST", "localhost")), port=int(os.getenv("MYSQL_PORT", "3306")),
        user=user, password=password, database=os.getenv("MYSQL_DATABASE", "ecommerce"), charset="utf8mb4",
        autocommit=autocommit, **options)


def connect_mongo(tz_aware=False):
    from pymongo import MongoClient

    if os.getenv("DOCDB_SECRET_ARN"):
        user, password = credentials("DOCDB_SECRET_ARN")
        # DocumentDB: TLS obligatorio, sin retryWrites, y la base admin no existe pero authSource=admin se acepta.
        return MongoClient(host=os.environ["DOCDB_HOST"], port=int(os.getenv("DOCDB_PORT", "27017")),
                           username=user, password=password, authSource="admin", tls=True,
                           tlsCAFile=os.environ["DOCDB_CA_BUNDLE"], retryWrites=False, directConnection=True,
                           tz_aware=tz_aware)
    return MongoClient(os.environ["MONGODB_URI"], tz_aware=tz_aware)


def connect_redshift():
    import redshift_connector

    user, password = credentials("REDSHIFT_SECRET_ARN")
    return redshift_connector.connect(
        host=os.environ["REDSHIFT_HOST"], port=int(os.getenv("REDSHIFT_PORT", "5439")),
        database=os.getenv("REDSHIFT_DATABASE", "dev"), user=user, password=password, ssl=True)
