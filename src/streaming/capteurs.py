"""Flux des capteurs avec Spark Structured Streaming (lot 5).

BG-42 (ce fichier, étape 1) : LIRE le flux déposé par tools/stream_replay.py dans
/data/landing/capteurs, typer les événements et repérer les lignes invalides.
Les étapes suivantes s'ajoutent ici : watermark, déduplication et fenêtres (BG-43),
alertes (BG-44), écriture idempotente et reprise (BG-45).

Lancement (dans le conteneur spark-client, le job part sur YARN) :
    # traite tous les fichiers déjà présents puis s'arrête (démonstration, tests)
    spark-submit src/streaming/capteurs.py --disponible
    # flux continu, arrêt automatique au bout de 120 s
    spark-submit src/streaming/capteurs.py --duree 120

Choix de lecture (mesurés sur l'échantillon J1 : 120 fichiers, 10 783 lignes) :
  - Schéma EXPLICITE : obligatoire en streaming (Spark ne peut pas deviner le schéma
    de fichiers qui n'existent pas encore) et seul moyen de garder les lignes invalides.
  - `value` lue en TEXTE puis convertie : 22 valeurs arrivent en texte ("58.14").
    Avec `value` déclarée en double, Spark rejette ces 22 lignes entières comme du JSON
    invalide (49 lignes corrompues au lieu de 27). En texte puis `cast`, on les garde.
  - `event_time` lue en texte puis convertie avec to_timestamp : le fuseau de la
    source (+01:00) est respecté, une date illisible devient un motif de rejet.
  - pathGlobFilter="*.jsonl" : `hdfs dfs -put` écrit d'abord <nom>._COPYING_ puis
    renomme. Le filtre garantit que Spark ne lit jamais un fichier à moitié copié.
  - maxFilesPerTrigger : borne la taille d'un micro-lot (12 fichiers = 1 min de flux),
    pour que le traitement reste régulier sur un petit cluster.
"""
import argparse
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

COLONNE_CORROMPUE = "_corrupt_record"

# Métriques connues et unités attendues (README du kit, profilage J1).
# Attention : la pression s'appelle "pression", pas "pressure".
METRIQUES = ("temperature", "vibration", "pression")

SCHEMA_CAPTEURS = T.StructType([
    T.StructField("event_id", T.StringType()),
    T.StructField("sensor_id", T.StringType()),
    T.StructField("machine_id", T.StringType()),
    T.StructField("site", T.StringType()),
    T.StructField("country", T.StringType()),
    T.StructField("metric", T.StringType()),
    T.StructField("value", T.StringType()),       # texte, converti ensuite (voir plus haut)
    T.StructField("unit", T.StringType()),
    T.StructField("event_time", T.StringType()),  # texte, converti ensuite
    T.StructField(COLONNE_CORROMPUE, T.StringType()),
])


def lire_flux(spark: SparkSession, landing: str, max_fichiers: int = 12) -> DataFrame:
    """Source streaming : les fichiers JSON Lines déposés dans le dossier d'atterrissage."""
    return (
        spark.readStream
        .schema(SCHEMA_CAPTEURS)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", COLONNE_CORROMPUE)
        .option("pathGlobFilter", "*.jsonl")
        .option("maxFilesPerTrigger", max_fichiers)
        .json(landing)
    )


def preparer_evenements(df: DataFrame) -> DataFrame:
    """Type les colonnes et calcule les motifs de rejet, en un seul passage.

    Même principe que le silver (src/silver/quality.py) : une colonne `motifs` (liste).
    Liste vide = événement valide. La fonction marche à l'identique sur un DataFrame
    batch (tests) ou streaming (production) : elle n'utilise que des fonctions natives.

    Motifs :
        JSON_INVALIDE         ligne illisible (_corrupt_record) : rien d'autre n'est contrôlé
        EVENT_ID_MANQUANT     event_id vide : impossible de dédoublonner (BG-43)
        DATE_INVALIDE         event_time vide ou illisible : impossible de fenêtrer (BG-43)
        METRIQUE_INCONNUE     metric hors temperature, vibration, pression
        VALEUR_MANQUANTE      value nulle ou vide
        VALEUR_NON_NUMERIQUE  value présente mais non convertible en nombre
    """
    corrompue = F.col(COLONNE_CORROMPUE).isNotNull()
    valeur_texte = F.trim(F.col("value"))
    df = (
        df.withColumn("_source_file", F.input_file_name())
        .withColumn("event_id", F.trim(F.col("event_id")))
        .withColumn("metrique", F.lower(F.trim(F.col("metric"))))
        .withColumn("valeur", valeur_texte.cast("double"))
        .withColumn("horodatage", F.to_timestamp(F.trim(F.col("event_time"))))
    )
    regles = [
        ("EVENT_ID_MANQUANT", F.col("event_id").isNull() | (F.col("event_id") == "")),
        ("DATE_INVALIDE", F.col("horodatage").isNull()),
        ("METRIQUE_INCONNUE", ~F.coalesce(F.col("metrique").isin(*METRIQUES), F.lit(False))),
        ("VALEUR_MANQUANTE", valeur_texte.isNull() | (valeur_texte == "")),
        ("VALEUR_NON_NUMERIQUE", (valeur_texte != "") & F.col("valeur").isNull()),
    ]
    motifs = F.when(corrompue, F.array(F.lit("JSON_INVALIDE"))).otherwise(
        F.filter(F.array(*[F.when(c, F.lit(nom)) for nom, c in regles]), lambda m: m.isNotNull())
    )
    return df.withColumn("motifs", motifs)


def bilan_du_lot(lot: DataFrame, id_lot: int) -> dict:
    """Comptages d'un micro-lot : lus, valides, invalides et détail par motif."""
    lignes = lot.select(F.size("motifs").alias("n"), "motifs").groupBy().agg(
        F.count(F.lit(1)).alias("lus"),
        F.sum(F.when(F.col("n") == 0, 1).otherwise(0)).alias("valides"),
    ).first()
    detail = {
        r["motif"]: r["nombre"]
        for r in lot.select(F.explode("motifs").alias("motif"))
        .groupBy("motif").agg(F.count(F.lit(1)).alias("nombre")).collect()
    }
    lus = lignes["lus"] or 0
    valides = lignes["valides"] or 0
    return {"lot": id_lot, "lus": lus, "valides": valides, "invalides": lus - valides,
            **dict(sorted(detail.items()))}


def afficher_bilan(lot: DataFrame, id_lot: int) -> None:
    """Appelé par Spark à chaque micro-lot (foreachBatch) : une ligne BILAN par lot."""
    bilan = bilan_du_lot(lot, id_lot)
    print("BILAN capteurs " + " ".join(f"{k}={v}" for k, v in bilan.items()), flush=True)


def analyser_arguments(argv=None):
    p = argparse.ArgumentParser(description="Lecture du flux capteurs (BG-42)")
    p.add_argument("--landing", default="/data/landing/capteurs")
    p.add_argument("--checkpoint", default="/checkpoints/capteurs/lecture",
                   help="état du flux (fichiers déjà lus), pour reprendre après un arrêt")
    p.add_argument("--max-fichiers", type=int, default=12, help="fichiers max par micro-lot")
    p.add_argument("--intervalle", default="10 seconds", help="déclenchement des micro-lots")
    groupe = p.add_mutually_exclusive_group()
    groupe.add_argument("--disponible", action="store_true",
                        help="traiter les fichiers déjà présents puis s'arrêter")
    groupe.add_argument("--duree", type=int, default=0,
                        help="arrêter le flux au bout de N secondes (0 = jamais)")
    return p.parse_args(argv)


def main(argv=None) -> int:
    args = analyser_arguments(argv)
    spark = (SparkSession.builder.appName("streaming-capteurs-lecture")
             .config("spark.ui.showConsoleProgress", "false")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    evenements = preparer_evenements(lire_flux(spark, args.landing, args.max_fichiers))
    ecriture = (evenements.writeStream
                .foreachBatch(afficher_bilan)
                .option("checkpointLocation", args.checkpoint))
    ecriture = (ecriture.trigger(availableNow=True) if args.disponible
                else ecriture.trigger(processingTime=args.intervalle))
    requete = ecriture.start()

    if args.disponible:
        requete.awaitTermination()
    elif args.duree > 0:
        requete.awaitTermination(args.duree)
        requete.stop()
    else:
        requete.awaitTermination()

    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
