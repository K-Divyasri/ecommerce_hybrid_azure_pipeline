"""Spark tests need Java, so they run inside the spark container (or GitHub Actions):

    docker compose run --rm spark python -m pytest tests/spark

On a host without pyspark they are skipped, not failed."""
import pytest

pyspark = pytest.importorskip("pyspark")


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession
    s = (SparkSession.builder.master("local[2]").appName("tests")
         .config("spark.sql.shuffle.partitions", "2")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.ui.enabled", "false").getOrCreate())
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()
