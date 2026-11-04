"""DAG de 7 pasos del data warehouse en AWS: RDS MySQL y DocumentDB -> S3 -> Redshift -> dbt -> pruebas -> registro.

Cada paso es una tarea (o un grupo de tareas) con dependencias explícitas, reintentos acotados y registros que
llevan el lote y la tarea. No hay intervención manual entre etapas.
"""
import os
from datetime import datetime, timedelta, timezone

from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.models.param import Param
from airflow.operators.python import get_current_context

from ecommerce_dw import pipeline

SCHEDULE = os.environ.get("DAG_SCHEDULE") or None  # se activa con ./infra/programar_dag.sh


@dag(
    dag_id="dw_pipeline",
    schedule=SCHEDULE,
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,  # nunca dos lotes escribiendo a la vez
    is_paused_upon_creation=True,
    params={
        "reprocess_batch_id": Param("", type="string",
                                    description="Vacío: nueva extracción. Con un batch_id: reprocesa ese lote desde S3 sin consultar las fuentes."),
        "simular_fallo": Param("", type="string", enum=["", "carga_redshift", "dbt_marts"],
                               description="Solo para demostraciones: provoca UN fallo controlado; el reintento se recupera."),
    },
    default_args={"retries": 2, "retry_delay": timedelta(minutes=1), "retry_exponential_backoff": True,
                  "max_retry_delay": timedelta(minutes=10)},
    tags=["dw", "aws", "redshift"],
)
def dw_pipeline():
    @task(task_id="1_validar_conexiones_y_parametros")
    def validar(params=None, dag_run=None):
        return pipeline.validate(params, dag_run)

    @task(task_id="2_extraer_mysql")
    def extraer_mysql(info):
        return pipeline.extract_mysql_step(info)

    @task(task_id="2_extraer_mongodb")
    def extraer_mongodb(info):
        return pipeline.extract_mongo_step(info)

    @task(task_id="3_escribir_s3")
    def escribir_s3(info, mysql_records, mongo_records):
        return pipeline.write_s3_step(info, mysql_records, mongo_records)

    @task(task_id="4_cargar_redshift")
    def cargar_redshift(info, batch, params=None):
        return pipeline.load_step(info, batch, params)

    @task(task_id="5_dbt_deps")
    def dbt_deps(info):
        return pipeline.dbt_step(info, "deps")

    @task(task_id="5_dbt_seed")
    def dbt_seed(info):
        return pipeline.dbt_step(info, "seed")

    @task(task_id="5_dbt_intermediate")
    def dbt_intermediate(info):
        return pipeline.dbt_step(info, "run", "path:models/intermediate")

    @task(task_id="5_dbt_quarantine")
    def dbt_quarantine(info):
        return pipeline.dbt_step(info, "run", "path:models/quarantine")

    @task(task_id="5_dbt_snapshot")
    def dbt_snapshot(info):
        return pipeline.dbt_step(info, "snapshot")

    @task(task_id="5_dbt_marts")
    def dbt_marts(info, params=None):
        summary = pipeline.dbt_step(info, "run", "path:models/marts", params, fail_stage="dbt_marts")
        pipeline.mark_transformed(info)
        return summary

    @task(task_id="6_pruebas_calidad")
    def pruebas(info):
        return pipeline.quality_step(info)

    @task(task_id="7_registrar_resultado", trigger_rule="all_done")
    def registrar(info):
        ctx = get_current_context()
        ti, dag_run = ctx["ti"], ctx["dag_run"]
        states = {t.task_id: str(t.state) for t in dag_run.get_task_instances() if t.task_id != "7_registrar_resultado"}
        results = {t: ti.xcom_pull(task_ids=t) for t in ("4_cargar_redshift", "5_dbt_marts", "6_pruebas_calidad")}
        info = dict(info or {})  # si el paso 1 falló, no hay información del lote: se conserva al menos la ejecución
        info.setdefault("airflow_run_id", dag_run.run_id)
        try:
            pipeline.record_result(info, states, results)
        except RuntimeError as error:
            raise AirflowFailException(str(error))

    info = validar()
    my, mo = extraer_mysql(info), extraer_mongodb(info)
    batch = escribir_s3(info, my, mo)
    loaded = cargar_redshift(info, batch)
    d1, d2, d3, d4, d5, d6 = (dbt_deps(info), dbt_seed(info), dbt_intermediate(info), dbt_quarantine(info),
                              dbt_snapshot(info), dbt_marts(info))
    loaded >> d1 >> d2 >> d3 >> d4 >> d5 >> d6
    checks = pruebas(info)
    d6 >> checks >> registrar(info)


dw_pipeline()
