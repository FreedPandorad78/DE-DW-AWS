"""Aplana documentos de MongoDB a las filas de raw (sin las columnas de metadatos)."""
from bson import json_util


def flatten_document(collection, document):
    customer = document.get("customer") or {}
    product = document.get("product") or {}
    if collection == "reviews":
        return (document.get("review_id"), customer.get("customer_id"), product.get("product_id"),
                document.get("rating"), document.get("reviewed_at"), json_util.dumps(document))
    device = document.get("device") or {}
    return (document.get("event_id"), document.get("event_type"), document.get("occurred_at"),
            customer.get("customer_id"), product.get("product_id"), device.get("sales_channel"),
            json_util.dumps(document))
