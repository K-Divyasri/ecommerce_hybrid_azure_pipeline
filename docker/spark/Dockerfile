# Spark 3.5.3 (Java 17, Python 3.8) + the Python libraries the jobs and tests need
# + the MySQL JDBC driver. Used by the compose service `spark` (one-off jobs and pytest).
FROM apache/spark:3.5.3

USER root
RUN pip install --no-cache-dir \
      "pandas==2.0.3" "numpy==1.24.4" "pyarrow==17.0.0" "pytest==8.3.3" \
      "requests==2.32.3" "SQLAlchemy==2.0.36" "PyMySQL==1.1.1" "matplotlib==3.7.5"
# pyspark itself comes from the image (/opt/spark/python); make it importable by plain python
ENV PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip:/opt/project
RUN mkdir -p /opt/jars && \
    curl -fsSL -o /opt/jars/mysql-connector-j.jar \
      https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.4.0/mysql-connector-j-8.4.0.jar
RUN ln -sf /usr/bin/python3 /usr/bin/python && mkdir -p /home/spark && chown -R spark:spark /home/spark
ENV HOME=/home/spark
USER spark
WORKDIR /opt/project
