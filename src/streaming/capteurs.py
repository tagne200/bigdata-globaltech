"""Flux des capteurs avec Spark Structured Streaming (lot 5).

Étapes du flux, une par ticket :
  BG-42  LIRE le flux déposé par tools/stream_replay.py dans /data/landing/capteurs,
         typer les événements et repérer les lignes invalides ;
  BG-43  WATERMARK de 6 minutes, DÉDUPLICATION des event_id, AGRÉGATS par fenêtre de 1 min ;
  BG-44  alertes sur seuils (à venir) ;
  BG-45  écriture idempotente et reprise après arrêt (à venir).

Lancement (dans le conteneur spark-client, le job part sur YARN) :
    # fenêtres (défaut) : traite les fichiers déjà présents puis s'arrête
    spark-submit src/streaming/capteurs.py --disponible
    # contrôle de la lecture seule (BG-42)
    spark-submit src/streaming/capteurs.py --etape lecture --disponible
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

# BG-43. Retard maximal mesuré sur J1 : 300 s (heure de l'événement -> heure du fichier).
# Le watermark doit être STRICTEMENT plus grand, sinon les événements les plus en retard
# sont écartés sans bruit. 6 min = 300 s mesurés + 1 min de marge.
WATERMARK = "6 minutes"
# 180 capteurs envoient chacun ~6 mesures par minute : une fenêtre de 1 min contient
# assez de points pour une moyenne utile, et reste assez courte pour réagir vite.
TAILLE_FENETRE = "1 minute"

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


def evenements_valides(df: DataFrame) -> DataFrame:
    """Ne garde que les événements sans motif de rejet."""
    return df.filter(F.size("motifs") == 0)


def dedoublonner(df: DataFrame, watermark: str = WATERMARK) -> DataFrame:
    """Watermark + suppression des event_id renvoyés (BG-43).

    - withWatermark : Spark garde en mémoire les événements récents tant que l'horloge
      des événements n'a pas dépassé « heure max vue - 6 min ». Au-delà, un événement
      plus ancien est considéré trop tard et écarté (compté dans les métriques).
    - dropDuplicatesWithinWatermark (Spark 3.5) : garde le premier event_id vu et oublie
      la clé une fois le watermark passé. Avec dropDuplicates classique, il faudrait mettre
      l'heure dans la clé ; un même event_id renvoyé à une autre heure ne serait plus
      reconnu (mesuré sur J1 : 201 event_id en double, tous avec la même heure).
    dropDuplicatesWithinWatermark n'existe qu'en streaming : sur un DataFrame batch (tests,
    contrôle de référence), on applique dropDuplicates, qui donne le même résultat.
    """
    if not df.isStreaming:
        return df.dropDuplicates(["event_id"])
    return df.withWatermark("horodatage", watermark).dropDuplicatesWithinWatermark(["event_id"])


def agreger_par_fenetre(df: DataFrame, taille: str = TAILLE_FENETRE) -> DataFrame:
    """Agrégats par capteur et par fenêtre fixe (tumbling) de 1 minute (BG-43).

    En mode append, une fenêtre n'est écrite qu'UNE fois, quand le watermark a dépassé sa
    fin : son résultat est alors définitif (aucun événement tardif ne peut plus la modifier).
    """
    return (
        df.groupBy(F.window("horodatage", taille).alias("fenetre"),
                   "site", "country", "machine_id", "sensor_id", "metrique", "unit")
        .agg(F.count(F.lit(1)).alias("nb_mesures"),
             F.round(F.avg("valeur"), 3).alias("valeur_moyenne"),
             F.min("valeur").alias("valeur_min"),
             F.max("valeur").alias("valeur_max"))
        .select(F.col("fenetre.start").alias("debut_fenetre"),
                F.col("fenetre.end").alias("fin_fenetre"),
                "site", "country", "machine_id", "sensor_id", "metrique", "unit",
                "nb_mesures", "valeur_moyenne", "valeur_min", "valeur_max")
    )


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


def afficher_fenetres(lot: DataFrame, id_lot: int) -> None:
    """Une ligne FENETRES par micro-lot : fenêtres devenues définitives dans ce lot."""
    # Heures formatées PAR SPARK (fuseau de session Europe/Paris). Un collect() les
    # convertirait dans le fuseau du conteneur (UTC) : 07:00 affiché au lieu de 08:00.
    r = lot.agg(F.count(F.lit(1)).alias("fenetres"), F.sum("nb_mesures").alias("mesures"),
                F.date_format(F.min("debut_fenetre"), "HH:mm").alias("de"),
                F.date_format(F.max("fin_fenetre"), "HH:mm").alias("a")).first()
    if r["fenetres"]:
        print(f"FENETRES capteurs lot={id_lot} fenetres={r['fenetres']} mesures={r['mesures']} "
              f"de={r['de']} a={r['a']}", flush=True)
    else:
        print(f"FENETRES capteurs lot={id_lot} fenetres=0 (watermark pas encore dépassé)",
              flush=True)


def lignes_ecartees_par_le_watermark(requete) -> int:
    """Somme des événements jugés trop en retard (métrique Spark numRowsDroppedByWatermark)."""
    return sum(op.get("numRowsDroppedByWatermark", 0)
               for p in requete.recentProgress for op in p.get("stateOperators", []))


def analyser_arguments(argv=None):
    p = argparse.ArgumentParser(description="Flux capteurs (BG-42 lecture, BG-43 fenêtres)")
    p.add_argument("--etape", choices=("lecture", "fenetres"), default="fenetres",
                   help="lecture : contrôle des lignes (BG-42) ; fenetres : agrégats (BG-43)")
    p.add_argument("--landing", default="/data/landing/capteurs")
    p.add_argument("--checkpoint", default=None,
                   help="état du flux (défaut : /checkpoints/capteurs/<etape>)")
    p.add_argument("--watermark", default=WATERMARK, help="retard toléré (défaut 6 minutes)")
    p.add_argument("--max-fichiers", type=int, default=12, help="fichiers max par micro-lot")
    p.add_argument("--intervalle", default="10 seconds", help="déclenchement des micro-lots")
    groupe = p.add_mutually_exclusive_group()
    groupe.add_argument("--disponible", action="store_true",
                        help="traiter les fichiers déjà présents puis s'arrêter")
    groupe.add_argument("--duree", type=int, default=0,
                        help="arrêter le flux au bout de N secondes (0 = jamais)")
    args = p.parse_args(argv)
    args.checkpoint = args.checkpoint or f"/checkpoints/capteurs/{args.etape}"
    return args


def main(argv=None) -> int:
    args = analyser_arguments(argv)
    spark = (SparkSession.builder.appName(f"streaming-capteurs-{args.etape}")
             .config("spark.ui.showConsoleProgress", "false")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    evenements = preparer_evenements(lire_flux(spark, args.landing, args.max_fichiers))
    if args.etape == "lecture":
        flux, traiter = evenements, afficher_bilan
    else:
        flux = agreger_par_fenetre(dedoublonner(evenements_valides(evenements), args.watermark))
        traiter = afficher_fenetres
    ecriture = (flux.writeStream
                .outputMode("append")
                .foreachBatch(traiter)
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

    if args.etape == "fenetres":
        print(f"WATERMARK capteurs retard_tolere={args.watermark} "
              f"evenements_ecartes_en_retard={lignes_ecartees_par_le_watermark(requete)}",
              flush=True)
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
