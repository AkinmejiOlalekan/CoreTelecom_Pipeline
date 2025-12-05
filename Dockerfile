FROM apache/airflow:3.0.6-python3.11


USER root


RUN apt-get update && apt-get install -y --no-install-recommends \
      build-essential \
      pkg-config \
      meson \
      ninja-build \
      dbus-x11 \
      python3-dev \
      libffi-dev \
      libssl-dev \
      libpq-dev \
    && apt-get clean && rm -rf /var/lib/apt/lists/*


USER airflow


WORKDIR /opt/airflow

COPY --chown=airflow:root ./requirements.txt /opt/airflow/requirements.txt

RUN pip install --upgrade pip setuptools wheel \
    && pip install --no-cache-dir apache-airflow-providers-fab \
    && pip install --no-cache-dir -r requirements.txt


COPY --chown=airflow:root ./extract_folder /opt/airflow/extract_folder
COPY --chown=airflow:root ./snowflakes /opt/airflow/snowflakes
COPY --chown=airflow:root ./airflow/dags /opt/airflow/dags
COPY --chown=airflow:root ./dbt /opt/airflow/dbt

ENV PATH="/home/airflow/.local/bin:${PATH}"
ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow:/opt/airflow/extract_folder:/opt/airflow/snowflakes"


RUN mkdir -p /opt/airflow/logs && \
    chmod -R 775 /opt/airflow/logs


EXPOSE 8080