"""Job silver des produits : historique SCD type 2 (ticket BG-32).

Lancement (dans le conteneur spark-client, sur YARN) :
    spark-submit src/silver/produits.py

Relit tout le bronze des produits (J1 = historique complet, J2 et J3 = nouvelles versions),
reconstruit l'historique et réécrit la table silver en entier : le rejeu ne duplique rien.
"""
import argparse
import sys
import uuid

from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.silver.scd2 import evaluer_produits, separer_produits


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--bronze", default="/data/bronze")
    p.add_argument("--silver", default="/data/silver/produits")
    p.add_argument("--rejets", default="/data/rejects/produits")
    a = p.parse_args(argv)

    spark = SparkSession.builder.appName("silver-produits").getOrCreate()
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    run_id = uuid.uuid4().hex[:12]

    bronze = spark.read.option("mergeSchema", True).parquet(f"{a.bronze}/produits")
    evalue = evaluer_produits(bronze)
    evalue.persist(StorageLevel.MEMORY_AND_DISK)
    versions, rejets, doublons = separer_produits(evalue, run_id)

    lues = bronze.count()
    n_versions, n_rejets, n_doublons = versions.count(), rejets.count(), doublons.count()

    versions.write.mode("overwrite").parquet(a.silver)
    rejets.write.mode("overwrite").partitionBy("livraison").parquet(a.rejets)

    # Relecture de contrôle : ce qui est réellement écrit sur HDFS.
    silver = spark.read.parquet(a.silver)
    ecrites = silver.count()
    produits = silver.select("product_id").distinct().count()
    courantes = silver.filter("is_current").count()
    print(f"BILAN produits run_id={run_id} lues={lues} versions={n_versions} rejets={n_rejets} "
          f"doublons={n_doublons} ecrites={ecrites} produits={produits} courantes={courantes}")

    # Une seule version courante par produit, et chaque ligne lue comptée une fois.
    if lues != n_versions + n_rejets + n_doublons or ecrites != n_versions or courantes != produits:
        print("ERREUR : bilan incohérent (lues, versions, courantes)", file=sys.stderr)
        sys.exit(1)
    evalue.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
