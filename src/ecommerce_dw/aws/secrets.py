"""Credenciales desde AWS Secrets Manager. Nada de contraseñas en el repositorio ni en archivos de la EC2."""
import json
import os

import boto3


def region():
    return os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION", "us-east-2")


def secret_json(secret_id):
    value = boto3.client("secretsmanager", region_name=region()).get_secret_value(SecretId=secret_id)["SecretString"]
    return json.loads(value)


def credentials(env_name):
    """Usuario y contraseña de un secreto cuyo ARN o nombre está en la variable de entorno env_name."""
    secret = secret_json(os.environ[env_name])
    return secret["username"], secret["password"]
