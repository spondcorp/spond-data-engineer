import logging
from typing import Union

from pyspark import SparkConf
from pyspark.sql import SparkSession


def get_spark_session(settings: Union[None, dict[str, str]] = None) -> SparkSession:
    """
    Helper function to build and return a Spark session with default settings.
    """
    if settings is None:
        settings = {}
    default_settings = {
        "spark.app.name": "spond-spark",
        "spark.default.parallelism": 1,
        "spark.dynamicAllocation.enabled": "false",
        "spark.executor.cores": 1,
        "spark.executor.instances": 1,
        "spark.io.compression.codec": "lz4",
        "spark.rdd.compress": "false",
        "spark.sql.shuffle.partitions": 1,
        "spark.shuffle.compress": "false",
        "spark.sql.session.timeZone": "UTC",
    }

    spark_conf_settings = {**default_settings, **settings}

    spark_conf = SparkConf().setAll(list(spark_conf_settings.items()))
    logging.info("Building Spark session")
    return SparkSession.builder.config(conf=spark_conf).master("local[1]").getOrCreate()
