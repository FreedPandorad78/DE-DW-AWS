"""Escritura de lotes en S3: raw/<fuente>/<entidad>/batch_id=<id>/. Nunca sobrescribe datos de otros lotes."""
import json
from pathlib import Path

from botocore.exceptions import ClientError


def entity_prefix(source, entity, batch_id):
    return f"raw/{source}/{entity}/batch_id={batch_id}/"


def batch_manifest_key(batch_id):
    return f"raw/_batches/batch_id={batch_id}/manifest.json"


def _exists(s3, bucket, key):
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as error:
        if error.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
            return False
        raise


def _read_json(s3, bucket, key):
    try:
        return json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read())
    except s3.exceptions.NoSuchKey:
        return None


def upload_batch(s3, bucket, batch_id, records):
    """Sube cada entidad del lote. El manifiesto se escribe al final de cada una: es su marca de confirmación.

    Si el lote ya está completo en S3 (un reintento), se reutilizan los archivos. Si existe con otro contenido,
    se detiene: otro proceso escribió datos distintos bajo el mismo lote.
    """
    published = []
    for record in records:
        prefix = entity_prefix(record["source"], record["entity"], batch_id)
        data_key, manifest_key = prefix + "data.csv", prefix + "manifest.json"
        previous = _read_json(s3, bucket, manifest_key)
        reused = False
        if previous is not None:
            if previous["sha256"] != record["sha256"]:
                raise RuntimeError(f"El lote {batch_id} ya existe en S3 con otro contenido: s3://{bucket}/{prefix}")
            reused = True
        else:
            if _exists(s3, bucket, data_key):
                raise RuntimeError(f"Hay un archivo sin manifiesto en s3://{bucket}/{data_key}; no se sobrescribe")
            s3.put_object(Bucket=bucket, Key=data_key, Body=Path(record["local_path"]).read_bytes(),
                          ContentType="text/csv")
        manifest = {k: v for k, v in record.items() if k != "local_path"}
        manifest.update(s3_bucket=bucket, s3_prefix=prefix, s3_key=data_key, reused=reused)
        if not reused:
            s3.put_object(Bucket=bucket, Key=manifest_key, Body=json.dumps(manifest, ensure_ascii=False, indent=2).encode(),
                          ContentType="application/json")
        published.append(manifest)
    batch = {"batch_id": batch_id, "entities": published, "rows_total": sum(m["rows"] for m in published)}
    s3.put_object(Bucket=bucket, Key=batch_manifest_key(batch_id),
                  Body=json.dumps(batch, ensure_ascii=False, indent=2).encode(), ContentType="application/json")
    return batch


def read_batch(s3, bucket, batch_id):
    manifest = _read_json(s3, bucket, batch_manifest_key(batch_id))
    if manifest is None:
        raise FileNotFoundError(f"El lote {batch_id} no existe en s3://{bucket}/raw/_batches/")
    return manifest
