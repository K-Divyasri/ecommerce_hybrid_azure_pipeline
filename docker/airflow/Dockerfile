# Official Airflow 3.1.8 image + Java (Spark runs in local mode inside the task) +
# pyspark + the MySQL JDBC driver. The base image already ships pandas, SQLAlchemy 2,
# PyMySQL, azure-storage-blob and azure-cosmos.
FROM apache/airflow:3.1.8-python3.11

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl \
 && apt-get clean && rm -rf /var/lib/apt/lists/*
RUN mkdir -p /opt/jars && \
    curl -fsSL -o /opt/jars/mysql-connector-j.jar \
      https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.4.0/mysql-connector-j-8.4.0.jar
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow
# pyspark 3.5.3 depends only on py4j; matplotlib reuses the numpy already installed.
# The usual advice is to install extras against Airflow's constraints file, but that
# file pins a different pyspark, so these two are pinned by hand instead.
RUN pip install --no-cache-dir "pyspark==3.5.3" "matplotlib==3.10.7"
