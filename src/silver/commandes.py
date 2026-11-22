"""Job silver des commandes : contrôles qualité et table de rejets (ticket BG-30).

Lancement (dans le conteneur spark-client, sur YARN) :
    spark-submit src/silver/commandes.py --livraison J1 --mois 2026-01

Lit le bronze sur HDFS (jamais data/), applique les contrôles en un seul passage,
écrit les lignes propres en silver et les rejets dans /data/rejects, puis vérifie
que lignes propres + rejets = lignes lues.
"""
import argparse
import sys
import uuid
from datetime import date

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark import StorageLevel

from src.silver.deduplication import marquer_doublons
from src.silver.quality import evaluer_commandes, extraire_doublons, separer


def bornes_du_mois(mois: str):
    """'2026-01' -> ('2026-01-01', '2026-02-01') : début inclus, fin exclue."""
    annee, m = (int(x) for x in mois.split("-"))
    debut = date(annee, m, 1)
    fin = date(annee + (m == 12), m % 12 + 1, 1)
    return debut.isoformat(), fin.isoformat()


def lire_bronze(spark, chemin, livraison=None):
    # mergeSchema est obligatoire (contrat du bronze) : une colonne ajoutée par une
    # livraison ultérieure (BG-35) ne serait pas vue sans lui.
    df = spark.read.option("mergeSchema", True).parquet(chemin)
    return df.where(F.col("livraison") == livraison) if livraison else df


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--livraison", required=True, help="J1, J2 ou J3")
    p.add_argument("--mois", required=True, help="mois de la livraison, ex. 2026-01")
    p.add_argument("--bronze", default="/data/bronze")
    p.add_argument("--silver", default="/data/silver/commandes")
    p.add_argument("--rejets", default="/data/rejects/commandes")
    a = p.parse_args(argv)

    spark = SparkSession.builder.appName(f"silver-commandes-{a.livraison}").getOrCreate()
    # Réécrit seulement la partition de cette livraison : relancer ne duplique rien.
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    run_id = uuid.uuid4().hex[:12]
    debut, fin = bornes_du_mois(a.mois)

    commandes = lire_bronze(spark, f"{a.bronze}/commandes", a.livraison)
    # Référentiels : toutes les livraisons déjà chargées (clés connues à ce jour).
    produits = lire_bronze(spark, f"{a.bronze}/produits")
    clients = lire_bronze(spark, f"{a.bronze}/clients")

    # 1) on repère les doublons, 2) on contrôle la qualité des lignes restantes
    evalue = evaluer_commandes(marquer_doublons(commandes), produits, clients, debut, fin)
    # persist : les deux sorties (propres, rejets) réutilisent le même calcul.
    evalue.persist(StorageLevel.MEMORY_AND_DISK)
    propres, rejets = separer(evalue, run_id)

    lues = commandes.count()
    n_propres = propres.count()
    n_rejets = rejets.count()
    doublons = extraire_doublons(evalue)
    n_dbl_exacts = doublons.filter(F.col("_doublon") == "EXACT").count()
    n_dbl_statut = doublons.filter(F.col("_doublon") == "STATUT").count()
    n_doublons = n_dbl_exacts + n_dbl_statut

    propres.write.mode("overwrite").partitionBy("ingest_date").parquet(a.silver)
    rejets.write.mode("overwrite").partitionBy("livraison").parquet(a.rejets)

    # Relecture de contrôle : on compte ce qui est RÉELLEMENT écrit sur HDFS.
    # `ecrites` doit égaler `propres` ; `total_*` ne doit pas grossir si on relance.
    silver_ecrite = spark.read.parquet(a.silver)
    ecrites = silver_ecrite.where(F.col("livraison") == a.livraison).count()
    total_silver = silver_ecrite.count()
    total_rejets = spark.read.parquet(a.rejets).count()

    print(f"BILAN commandes {a.livraison} run_id={run_id} periode=[{debut}, {fin}[ "
          f"lues={lues} propres={n_propres} rejets={n_rejets} "
          f"doublons={n_doublons} (exacts={n_dbl_exacts} statut={n_dbl_statut}) "
          f"ecrites={ecrites} total_silver={total_silver} total_rejets={total_rejets}")
    print("Motifs de rejet (une ligne peut en cumuler plusieurs) :")
    (rejets.select(F.explode("motifs").alias("motif")).groupBy("motif").count()
     .orderBy(F.desc("count")).show(truncate=False))

    if lues != n_propres + n_rejets + n_doublons or ecrites != n_propres:
        print("ERREUR : propres + rejets + doublons != lues, ou silver écrite != propres",
              file=sys.stderr)
        sys.exit(1)
    evalue.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()