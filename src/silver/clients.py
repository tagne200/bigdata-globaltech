"""Job silver des clients : doublons, normalisation et pseudonymisation (ticket BG-33).

Lancement (dans le conteneur spark-client, sur YARN), le sel passé par l'environnement :
    docker compose exec -e PSEUDO_SEL spark-client spark-submit src/silver/clients.py

Relit tout le bronze des clients (J1 = extraction complète, J2 et J3 = deltas) et réécrit la
table silver en entier : une mise à jour remplace l'ancienne version du client, et relancer
ne duplique rien.
"""
import argparse
import sys
import uuid

from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.silver.nettoyage_clients import evaluer_clients, separer_clients
from src.silver.pseudonymisation import lire_sel, table_du_sel


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--bronze", default="/data/bronze")
    p.add_argument("--silver", default="/data/silver/clients")
    p.add_argument("--rejets", default="/data/rejects/clients")
    a = p.parse_args(argv)

    sel = lire_sel()  # s'arrête tout de suite, avec un message clair, si le sel manque
    spark = SparkSession.builder.appName("silver-clients").getOrCreate()
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    run_id = uuid.uuid4().hex[:12]

    bronze = spark.read.option("mergeSchema", True).parquet(f"{a.bronze}/clients")
    evalue = evaluer_clients(bronze, table_du_sel(spark, sel))
    evalue.persist(StorageLevel.MEMORY_AND_DISK)
    propres, rejets, doublons = separer_clients(evalue, run_id)

    lues = bronze.count()
    n_propres, n_rejets = propres.count(), rejets.count()
    n_exacts = doublons.filter(F.col("_doublon") == "EXACT").count()
    n_anciens = doublons.filter(F.col("_doublon") == "ANCIEN").count()
    n_doublons = n_exacts + n_anciens
    gardees = evalue.filter(F.col("_doublon").isNull() & (F.size("motifs") == 0))
    sans_email = gardees.filter(F.col("_email").isNull() | (F.col("_email") == "")).count()
    email_invalide = gardees.filter(F.col("_email") != "").filter(~F.col("_email_ok")).count()
    sans_pays = propres.filter(F.col("country").isNull()).count()
    sans_segment = propres.filter(F.col("segment").isNull()).count()

    propres.write.mode("overwrite").parquet(a.silver)
    rejets.write.mode("overwrite").partitionBy("livraison").parquet(a.rejets)

    silver = spark.read.parquet(a.silver)  # relecture de contrôle
    ecrites = silver.count()
    distincts = silver.select("customer_id").distinct().count()
    print(f"BILAN clients run_id={run_id} lues={lues} propres={n_propres} rejets={n_rejets} "
          f"doublons={n_doublons} (exacts={n_exacts} anciens={n_anciens}) ecrites={ecrites} "
          f"clients={distincts} sans_email={sans_email} email_invalide={email_invalide} "
          f"sans_pays={sans_pays} sans_segment={sans_segment}")

    if lues != n_propres + n_rejets + n_doublons or ecrites != n_propres or distincts != ecrites:
        print("ERREUR : bilan incohérent (lues, écrites, ou customer_id en double)", file=sys.stderr)
        sys.exit(1)
    evalue.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
