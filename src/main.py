import json
import os

from pyspark.sql import SparkSession

CONF = os.environ.get("APP_CONF", "/opt/project/conf/app.json")


def build_spark(conf):
    return (SparkSession.builder.appName(conf["app_name"])
            .config("spark.sql.shuffle.partitions", conf["shuffle_partitions"])
            .getOrCreate())


def main():
    conf = json.load(open(CONF, encoding="utf-8"))
    spark = build_spark(conf)
    base = f"{conf['input_dir']}/{conf['delivery']}"
    # TODO: ingestion Bronze -> nettoyage Silver -> agrégats Gold
    # Attention : inferSchema n'est qu'un outil d'exploration, l'énoncé impose des schémas explicites.
    commandes = spark.read.option("header", True).csv(f"{base}/commandes")
    commandes.printSchema()
    print("lignes commandes :", commandes.count())
    spark.stop()


if __name__ == "__main__":
    main()
