FROM apache/airflow:3.3.1-python3.12

ENV PIP_PROGRESS_BAR=off

COPY --chown=airflow:root . /opt/carrier-risk

WORKDIR /opt/carrier-risk

RUN pip install --no-cache-dir --no-build-isolation \
    -e '.[dbt,airflow]'
