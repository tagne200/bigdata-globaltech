
"""BG-34 : traitement Silver des logs web sur Spark YARN."""

import sys
import uuid

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.silver.parsing_logs import parser_logs


BRONZE = "/data/bronze/logs_web"
SILVER = "/data/silver/logs_web"
REJETS = "/data/rejects/logs_web"

# Dates logiques des livraisons, définies par l'équipe.
LIVRAISONS = {
    "J1": "2026-02-01",
    "J2": "2026-03-01",
    "J3": "2026-04-01",
}


def traiter_logs(spark, livraison):
    """Parse les logs Bronze, sépare Silver et rejets."""

    if livraison not in LIVRAISONS:
        raise ValueError(f"Livraison inconnue : {livraison}")

    # Écraser uniquement les partitions concernées.
    spark.conf.set(
        "spark.sql.sources.partitionOverwriteMode",
        "dynamic"
    )

    date_ingestion = LIVRAISONS[livraison]
    run_id = uuid.uuid4().hex[:12]

    # Lire uniquement la livraison demandée depuis Bronze.
    bronze = (
        spark.read.parquet(BRONZE)
        .filter(F.col("ingest_date") == date_ingestion)
        .filter(F.col("livraison") == livraison)
    )

    # Transformer chaque ligne avec regexp_extract.
    resultat = (
        parser_logs(bronze)
        .withColumn("run_id", F.lit(run_id))
        .cache()
    )

    try:
        # Séparer les lignes selon leur validité.
        propres = resultat.filter(
            F.col("motif_rejet").isNull()
        )

        rejets = resultat.filter(
            F.col("motif_rejet").isNotNull()
        )

        nb_lues = resultat.count()
        nb_propres = propres.count()
        nb_rejets = rejets.count()

        if nb_lues == 0:
            raise RuntimeError(
                f"Aucun log Bronze trouvé pour {livraison}"
            )

        # Contrôle de conservation de toutes les lignes.
        if nb_lues != nb_propres + nb_rejets:
            raise RuntimeError(
                "Bilan incohérent : lignes perdues"
            )

        # Silver : colonnes structurées, sans ligne brute.
        (
            propres.drop("ligne", "motif_rejet", "run_id")
            .write
            .mode("overwrite")
            .partitionBy("livraison")
            .parquet(SILVER)
        )

        # Rejets : conserver la ligne brute et son motif.
        (
            rejets.drop("livraison")
            .write
            .mode("overwrite")
            .parquet(f"{REJETS}/livraison={livraison}")
        )

        # Vérifier ce qui est réellement écrit sur HDFS.
        nb_ecrites = spark.read.parquet(
            f"{SILVER}/livraison={livraison}"
        ).count()

        nb_rejets_ecrits = spark.read.parquet(
            f"{REJETS}/livraison={livraison}"
        ).count()

        if (nb_ecrites != nb_propres
                or nb_rejets_ecrits != nb_rejets):
            raise RuntimeError(
                "Les comptages HDFS ne correspondent pas"
            )

        print(
            f"BILAN logs_web run_id={run_id} "
            f"livraison={livraison} "
            f"lues={nb_lues} "
            f"propres={nb_propres} "
            f"rejets={nb_rejets} "
            f"ecrites={nb_ecrites} "
            f"rejets_ecrits={nb_rejets_ecrits}",
            flush=True
        )

    finally:
        resultat.unpersist()


if __name__ == "__main__":
    livraison = sys.argv[1] if len(sys.argv) > 1 else "J1"

    spark = (
        SparkSession.builder
        .appName(f"BG-34-logs-web-{livraison}")
        .getOrCreate()
    )

    try:
        traiter_logs(spark, livraison)
    finally:
        spark.stop()
