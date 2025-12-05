FROM jupyter/pyspark-notebook:latest

USER root

RUN apt-get update && apt-get install -y \
    libpq-dev \
    gcc \
    && apt-get clean

USER ${NB_UID}

RUN pip install --no-cache-dir \
    redis \
    kafka-python \
    psycopg2-binary \
    sqlalchemy \
    pandas \
    matplotlib \
    seaborn