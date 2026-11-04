FROM apache/airflow:2.11.2-python3.11

ARG AIRFLOW_VERSION=2.11.2
ARG CONSTRAINTS_PYTHON=3.11

# Certificado de AWS para TLS con RDS y DocumentDB.
USER root
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /opt/certs \
    && curl -fsSL https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem -o /opt/certs/global-bundle.pem
USER airflow

RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${CONSTRAINTS_PYTHON}.txt" \
    "apache-airflow==${AIRFLOW_VERSION}" \
    "Faker==37.0.0" \
    "PyMySQL==1.1.2" \
    "pymongo==4.16.0" \
    "redshift_connector" \
    && pip check

# dbt en su propio entorno virtual: sus dependencias no se mezclan con las de Airflow.
RUN python -m venv /home/airflow/dbt-venv \
    && /home/airflow/dbt-venv/bin/pip install --no-cache-dir \
      "dbt-core==1.9.4" \
      "dbt-redshift==1.9.5" \
    && /home/airflow/dbt-venv/bin/pip check \
    && ln -s /home/airflow/dbt-venv/bin/dbt /home/airflow/.local/bin/dbt
