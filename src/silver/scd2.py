"""Historisation des produits en SCD type 2 (ticket BG-32).


Un produit peut avoir plusieurs versions (prix, famille). On garde TOUTES les versions,
chacune avec sa période de validité :

    valid_from  date de la version (colonne updated_at de la source)
    valid_to    début de la version suivante (exclu), ou 9999-12-31 pour la version courante
    Règle de jointure : valid_from <= order_ts < valid_to.
    is_current  True pour la dernière version de chaque produit

Fusion des deltas (J2, J3) : on relit TOUT le bronze des produits à chaque exécution et on
reconstruit l'historique complet. Une nouvelle version n'a donc rien de spécial : la version
qui était courante reçoit sa date de fin, la nouvelle devient courante. Relancer ne duplique
rien (la table est réécrite en entier).

Motifs de rejet :
    FORMAT_INVALIDE      ligne mal formée en bronze (_corrupt_record)
    PRODUIT_ID_INVALIDE  product_id vide
    DATE_INVALIDE        updated_at illisible
    PRIX_INVALIDE        prix illisible ou <= 0
    FAMILLE_INVALIDE     famille vide
Doublon : même (product_id, valid_from) vu plusieurs fois (livraisons rejouées, ré-extractions).
On garde la ligne extraite en dernier (extracted_at le plus récent).
"""
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (BooleanType, DateType, StringType, StructField, StructType,
                               TimestampType)

from src.silver.dates import parse_date, parse_order_ts
from src.silver.nombres import parse_price
from src.silver.schemas import PRICE_TYPE

DATE_FIN = "9999-12-31"

SILVER_PRODUITS_SCD2 = StructType([
    StructField("product_id", StringType(), False),
    StructField("product_name", StringType(), True),
    StructField("family", StringType(), True),
    StructField("category", StringType(), True),
    StructField("unit_price_eur", PRICE_TYPE, True),
    StructField("valid_from", DateType(), False),
    StructField("valid_to", DateType(), False),
    StructField("is_current", BooleanType(), False),
    StructField("extracted_at", TimestampType(), True),
])

COLONNES_SOURCE = ["product_id", "product_name", "family", "category", "unit_price_eur",
                   "updated_at", "extracted_at"]


def normaliser_famille(col):
    """'  DISTRIBUTION ÉLECTRIQUE ' et 'distribution électrique' -> 'Distribution Électrique'."""
    return F.initcap(F.trim(col))


def evaluer_produits(produits: DataFrame) -> DataFrame:
    """Ajoute les valeurs converties, `motifs`, `_doublon`, `valid_to` et `is_current`."""
    corrompue = (F.col("_corrupt_record").isNotNull() if "_corrupt_record" in produits.columns
                 else F.lit(False))
    df = (produits
          .withColumn("_rid", F.monotonically_increasing_id().cast("string"))
          .withColumn("_pid", F.trim(F.col("product_id")))
          .withColumn("_famille", normaliser_famille(F.col("family")))
          .withColumn("_prix", parse_price(F.col("unit_price_eur")))
          .withColumn("_debut", parse_date(F.col("updated_at")))
          .withColumn("_extrait", parse_order_ts(F.col("extracted_at"))))

    regles = [
        ("PRODUIT_ID_INVALIDE", F.col("_pid").isNull() | (F.col("_pid") == "")),
        ("DATE_INVALIDE", F.col("_debut").isNull()),
        ("PRIX_INVALIDE", F.col("_prix").isNull() | (F.col("_prix") <= 0)),
        ("FAMILLE_INVALIDE", F.col("_famille").isNull() | (F.col("_famille") == "")),
    ]
    motifs = F.when(corrompue, F.array(F.lit("FORMAT_INVALIDE"))).otherwise(
        F.filter(F.array(*[F.when(c, F.lit(n)) for n, c in regles]), lambda m: m.isNotNull()))
    df = df.withColumn("motifs", motifs)

    # Doublons : parmi les lignes valides, une seule par (produit, date de version).
    # Les lignes rejetées ont leur propre clé : elles ne participent à rien.
    valide = F.size("motifs") == 0
    cle_version = F.when(valide, F.concat_ws("|", F.col("_pid"), F.col("_debut").cast("string"))) \
        .otherwise(F.concat(F.lit("x-"), F.col("_rid")))
    df = df.withColumn("_rn", F.row_number().over(
        Window.partitionBy(cle_version).orderBy(F.col("_extrait").desc_nulls_last(), F.col("_rid"))))
    df = df.withColumn("_doublon", valide & (F.col("_rn") > 1))

    # SCD2 sur les versions retenues : la date de fin est la veille de la version suivante.
    retenue = valide & ~F.col("_doublon")
    cle_produit = F.when(retenue, F.col("_pid")).otherwise(F.concat(F.lit("x-"), F.col("_rid")))
    suivante = F.lead("_debut").over(Window.partitionBy(cle_produit).orderBy("_debut"))
    return (df.withColumn("_suivante", suivante)
            .withColumn("valid_to", F.coalesce(F.col("_suivante"),
                                               F.lit(DATE_FIN).cast("date")))
            .withColumn("is_current", F.col("_suivante").isNull())
            .drop("_rn", "_suivante"))


def separer_produits(evalue: DataFrame, run_id: str):
    """Renvoie (versions, rejets, doublons)."""
    versions = (evalue.filter((F.size("motifs") == 0) & ~F.col("_doublon"))
                .select(F.col("_pid").alias("product_id"), "product_name",
                        F.col("_famille").alias("family"), "category",
                        F.col("_prix").alias("unit_price_eur"),
                        F.col("_debut").alias("valid_from"), "valid_to", "is_current",
                        F.col("_extrait").alias("extracted_at"), "livraison"))
    versions = versions.select(
        *[F.col(f.name).cast(f.dataType).alias(f.name) for f in SILVER_PRODUITS_SCD2.fields],
        "livraison")
    cols_source = [c for c in COLONNES_SOURCE if c in evalue.columns]
    if "_corrupt_record" in evalue.columns:
        cols_source.append("_corrupt_record")
    rejets = evalue.filter(F.size("motifs") > 0).select(
        F.lit("produits").alias("source"), "livraison", "motifs",
        F.to_json(F.struct(*cols_source)).alias("ligne_origine"),
        F.lit(run_id).alias("run_id"), F.current_timestamp().alias("rejete_le"))
    doublons = evalue.filter(F.col("_doublon"))
    return versions, rejets, doublons
