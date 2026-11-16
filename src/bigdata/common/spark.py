"""Construction de la session Spark à partir de la configuration.

Le master n'est pas fixé ici : il est donné par spark-submit (--master yarn)
sur le cluster, et par les tests (local[1]) en CI.
"""

from __future__ import annotations

from pyspark.sql import SparkSession

TIMEZONE = "Europe/Paris"


def build_spark(conf: dict, master: str | None = None) -> SparkSession:
    builder = (
        SparkSession.builder.appName(conf["app_name"])
        .config("spark.sql.shuffle.partitions", str(conf["shuffle_partitions"]))
        .config("spark.sql.session.timeZone", TIMEZONE)
        # Rejeu sans doublon : seule la partition réécrite est remplacée.
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    )
    if master:
        builder = builder.master(master)
    return builder.getOrCreate()
