"""Ingestion de la couche bronze (lot 2, BG-24).

Lit une livraison (J1, J2 ou J3) déposée dans la zone d'atterrissage HDFS
(/data/landing/batch/<livraison>/<source>) et l'écrit, sans aucune
transformation métier, dans la couche bronze :

    /data/bronze/<source>/ingest_date=AAAA-MM-JJ/part-*.parquet

Principes (justifiés dans docs/adr/003-ingestion-bronze.md) :
  1. Sans perte : toutes les colonnes sont lues en texte (StringType). Une ligne
     mal formée n'est pas jetée : elle est gardée telle quelle dans la colonne
     _corrupt_record. Le tri entre lignes valides et rejets se fait en silver.
  2. Traçable : chaque ligne reçoit _source_file (fichier d'origine),
     _ingest_ts (heure d'exécution) et livraison (J1, J2 ou J3).
  3. Idempotent : la partition ingest_date est la date LOGIQUE de la livraison
     (J1 = 2026-02-01), jamais l'heure système. Avec
     partitionOverwriteMode=dynamic, relancer le job remplace uniquement la
     partition de cette livraison : pas de doublon, les autres livraisons ne
     sont pas touchées.

Lancement (dans le conteneur spark-client, le job part sur YARN) :
    spark-submit src/bronze/ingestion.py --livraison J1
    spark-submit src/bronze/ingestion.py --livraison J1 --sources commandes,clients
"""

import argparse
import logging
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

# Date logique de chaque livraison : le 1er du mois qui suit les commandes livrées.
# J1 = commandes de janvier 2026, livrées le 1er février, etc.
# (à déplacer dans la configuration externalisée au ticket BG-54)
DATES_LIVRAISON = {
    "J1": "2026-02-01",
    "J2": "2026-03-01",
    "J3": "2026-04-01",
}

# Sources batch et format brut. Les capteurs n'en font pas partie :
# ils arrivent en flux continu et sont traités au lot 5 (streaming).
SOURCES = {
    "commandes": "csv",
    "produits": "csv",
    "clients": "csv",
    "logs_web": "texte",
}

COLONNE_CORROMPUE = "_corrupt_record"

log = logging.getLogger("bronze")


def schema_texte(colonnes):
    """Schéma explicite : toutes les colonnes de l'en-tête en texte, plus _corrupt_record.

    Lire en texte garantit qu'aucune valeur n'est perdue ni convertie à tort
    (exemples réels de J1 : quantité "2 pcs", prix "205,16", date "hier").
    """
    champs = [T.StructField(nom, T.StringType(), True) for nom in colonnes]
    champs.append(T.StructField(COLONNE_CORROMPUE, T.StringType(), True))
    return T.StructType(champs)


def lire_csv(spark: SparkSession, chemin: str) -> DataFrame:
    """Lit tous les CSV d'un dossier en texte, en gardant les lignes mal formées."""
    # 1) Les noms de colonnes viennent de l'en-tête des fichiers eux-mêmes : si une
    #    livraison ajoute une colonne (changement de schéma annoncé par l'énoncé),
    #    elle est conservée automatiquement en bronze.
    colonnes = spark.read.option("header", True).csv(chemin).columns
    # 2) Relecture avec un schéma explicite. En mode PERMISSIVE, une ligne qui n'a
    #    pas le bon nombre de colonnes (séparateur ";", colonnes en trop ou en
    #    moins) est recopiée en entier dans _corrupt_record au lieu d'être perdue.
    return (
        spark.read.schema(schema_texte(colonnes))
        .option("header", True)          # en-tête répété dans chaque fichier part
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", COLONNE_CORROMPUE)
        .option("encoding", "UTF-8")
        .option("multiLine", False)
        .csv(chemin)
    )


def lire_texte(spark: SparkSession, chemin: str) -> DataFrame:
    """Lit des fichiers texte ligne à ligne (logs web au format Apache combined)."""
    return spark.read.text(chemin).withColumnRenamed("value", "ligne")


def ajouter_colonnes_techniques(df: DataFrame, livraison: str, ingest_date: str) -> DataFrame:
    """Ajoute les colonnes de traçabilité et la colonne de partition."""
    return (
        df.withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("livraison", F.lit(livraison))
        .withColumn("ingest_date", F.lit(ingest_date))
    )


def ingerer_source(spark, source, livraison, ingest_date, landing, bronze):
    """Ingère une source et renvoie son bilan (lignes lues, écrites, total de la table)."""
    entree = f"{landing}/{livraison}/{source}"
    sortie = f"{bronze}/{source}"

    brut = lire_csv(spark, entree) if SOURCES[source] == "csv" else lire_texte(spark, entree)
    df = ajouter_colonnes_techniques(brut, livraison, ingest_date)

    # Nombre de lignes de la source, compté avant l'écriture (référence du contrôle).
    # Pas de cache() : avec des exécuteurs de 512 Mo, garder 4 millions de lignes
    # en mémoire coûterait plus cher que relire les CSV une seconde fois.
    lues = df.count()

    # Écriture : mode overwrite + partitionOverwriteMode=dynamic (réglé sur la
    # session) = seule la partition ingest_date=<date de la livraison> est remplacée.
    df.write.mode("overwrite").partitionBy("ingest_date").parquet(sortie)

    # Contrôle après écriture : on relit ce qui est réellement sur HDFS.
    # Les lignes mal formées sont comptées ICI et pas sur le CSV : si une requête
    # sur le CSV ne lit que _corrupt_record, Spark n'analyse plus les autres
    # colonnes (column pruning) et ne détecte plus aucune ligne mal formée
    # (mesuré : 0 au lieu de 56 sur l'échantillon J1).
    table = spark.read.parquet(sortie)
    partition = table.filter(F.col("ingest_date") == ingest_date)
    ecrites = partition.count()
    mal_formees = (
        partition.filter(F.col(COLONNE_CORROMPUE).isNotNull()).count()
        if COLONNE_CORROMPUE in table.columns
        else 0
    )
    total = table.count()

    bilan = {
        "source": source,
        "livraison": livraison,
        "ingest_date": ingest_date,
        "lues": lues,
        "mal_formees": mal_formees,
        "ecrites": ecrites,
        "total_table": total,
    }
    # Ligne au format fixe, exploitée par scripts/preuve_rejeu_bronze.sh (BG-25).
    print("BILAN " + " ".join(f"{k}={v}" for k, v in bilan.items()), flush=True)
    return bilan


def construire_session(nom: str) -> SparkSession:
    return (
        SparkSession.builder.appName(nom)
        # Écrasement par partition : clé de l'idempotence (voir ADR-003).
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        # Parquet compressé en snappy (valeur par défaut, explicitée pour le rapport).
        .config("spark.sql.parquet.compression.codec", "snappy")
        # Pas de barre de progression : journal lisible et lignes BILAN exploitables.
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


def analyser_arguments(argv=None):
    p = argparse.ArgumentParser(description="Ingestion bronze d'une livraison (BG-24)")
    p.add_argument("--livraison", required=True, choices=sorted(DATES_LIVRAISON))
    p.add_argument(
        "--sources",
        default=",".join(SOURCES),
        help="liste séparée par des virgules (défaut : toutes les sources batch)",
    )
    p.add_argument(
        "--ingest-date",
        default=None,
        help="force la date de partition (défaut : date logique de la livraison)",
    )
    p.add_argument("--landing", default="/data/landing/batch", help="racine d'atterrissage")
    p.add_argument("--bronze", default="/data/bronze", help="racine de la couche bronze")
    args = p.parse_args(argv)

    args.sources = [s.strip() for s in args.sources.split(",") if s.strip()]
    inconnues = [s for s in args.sources if s not in SOURCES]
    if inconnues:
        p.error(f"sources inconnues : {inconnues} (possibles : {list(SOURCES)})")
    args.ingest_date = args.ingest_date or DATES_LIVRAISON[args.livraison]
    return args


def main(argv=None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s : %(message)s")
    args = analyser_arguments(argv)
    spark = construire_session(f"bronze-ingestion-{args.livraison}")
    spark.sparkContext.setLogLevel("WARN")
    log.info("livraison %s -> ingest_date=%s, sources=%s", args.livraison, args.ingest_date, args.sources)

    erreurs = 0
    for source in args.sources:
        bilan = ingerer_source(spark, source, args.livraison, args.ingest_date, args.landing, args.bronze)
        if bilan["ecrites"] != bilan["lues"]:
            log.error("%s : %d lignes lues mais %d écrites", source, bilan["lues"], bilan["ecrites"])
            erreurs += 1
        else:
            log.info("%s : %d lignes ingérées (dont %d mal formées, gardées)", source, bilan["lues"], bilan["mal_formees"])

    spark.stop()
    return 1 if erreurs else 0


if __name__ == "__main__":
    sys.exit(main())
