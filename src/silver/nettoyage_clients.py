"""Nettoyage des clients : doublons, normalisation, rejets et pseudonymisation (ticket BG-33).

Ordre des opérations :
  1. normaliser : email en minuscules, espaces retirés, pays en code ISO, dates converties ;
  2. repérer les doublons : exact (après normalisation), puis même customer_id -> on garde
     la ligne au updated_at le plus récent ;
  3. contrôler les lignes gardées (motifs de rejet) ;
  4. pseudonymiser : email -> email_hash, et suppression de nom, prénom, téléphone.

Motifs de rejet :
    FORMAT_INVALIDE     ligne mal formée en bronze (_corrupt_record)
    CLIENT_ID_INVALIDE  customer_id vide
    DATE_INVALIDE       updated_at illisible (impossible de choisir la version récente)
Email absent ou mal formé : le client est GARDÉ (son segment et son pays servent aux indicateurs),
mais son email_hash est vide : on ne pseudonymise pas une valeur qui n'existe pas ou qui est
fausse. Ces cas sont comptés dans le bilan (sans_email, email_invalide).
Doublons : EXACT (identique après normalisation) ou ANCIEN (même client, mise à jour plus vieille).

Données personnelles : la table de rejets ne contient NI nom, NI prénom, NI téléphone, NI email.
La ligne complète reste consultable en bronze, qui a ses propres droits d'accès.
"""
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (DateType, StringType, StructField, StructType, TimestampType)

from src.silver.dates import parse_date_multi, parse_order_ts
from src.silver.pays import normaliser_pays
from src.silver.pseudonymisation import email_hache

FORME_EMAIL = r"^[^@\s]+@[^@\s]+\.[^@\s]+$"

# Colonnes supprimées en silver : données personnelles directement identifiantes.
COLONNES_SUPPRIMEES = ["contact_first_name", "contact_last_name", "phone", "email"]

SILVER_CLIENTS_PSEUDO = StructType([
    StructField("customer_id", StringType(), False),
    StructField("company_name", StringType(), True),
    StructField("email_hash", StringType(), True),   # vide si l'email est absent ou mal formé
    StructField("country", StringType(), True),
    StructField("city", StringType(), True),
    StructField("segment", StringType(), True),
    StructField("created_at", DateType(), True),
    StructField("updated_at", TimestampType(), False),
])


def _vide_en_null(col):
    """'' ou '   ' -> null (valeur manquante)."""
    s = F.trim(col)
    return F.when(s != "", s)


def evaluer_clients(clients: DataFrame, sel: DataFrame) -> DataFrame:
    """Ajoute les valeurs normalisées, `_doublon` ('EXACT', 'ANCIEN' ou null) et `motifs`."""
    corrompue = (F.col("_corrupt_record").isNotNull() if "_corrupt_record" in clients.columns
                 else F.lit(False))
    df = (clients.withColumn("_rid", F.monotonically_increasing_id().cast("string"))
          .withColumn("_id", F.trim("customer_id"))
          .withColumn("_email", F.lower(F.trim("email")))
          .withColumn("_societe", _vide_en_null(F.col("company_name")))
          .withColumn("_prenom", _vide_en_null(F.col("contact_first_name")))
          .withColumn("_nom", _vide_en_null(F.col("contact_last_name")))
          .withColumn("_tel", _vide_en_null(F.col("phone")))
          .withColumn("_ville", _vide_en_null(F.col("city")))
          .withColumn("_segment", _vide_en_null(F.col("segment")))
          .withColumn("_pays", normaliser_pays(F.col("country")))
          .withColumn("_cree", parse_date_multi(F.col("created_at")))
          .withColumn("_maj", parse_order_ts(F.col("updated_at"))))

    # Étape 1 : doublons exacts APRÈS normalisation (espaces et casse ne comptent pas).
    contenu = F.concat_ws("\u0001", *[F.coalesce(F.col(c).cast("string"), F.lit("<NULL>")) for c in
                                      ["_id", "_societe", "_prenom", "_nom", "_email", "_tel",
                                       "_pays", "_ville", "_segment", "_cree", "_maj"]])
    cle_exacte = F.when(corrompue, F.concat(F.lit("corrompue-"), F.col("_rid"))) \
        .otherwise(F.sha2(contenu, 256))
    df = df.withColumn("_rn_exact", F.row_number().over(
        Window.partitionBy(cle_exacte).orderBy("_rid")))

    # Étape 2 : même customer_id -> on garde le updated_at le plus récent.
    en_jeu = (F.col("_rn_exact") == 1) & ~corrompue & F.col("_id").isNotNull() & (F.col("_id") != "")
    cle_client = F.when(en_jeu, F.concat(F.lit("c-"), F.col("_id"))) \
        .otherwise(F.concat(F.lit("x-"), F.col("_rid")))
    df = df.withColumn("_rn_client", F.row_number().over(
        Window.partitionBy(cle_client).orderBy(F.col("_maj").desc_nulls_last(), F.col("_rid"))))
    df = df.withColumn("_doublon", F.when(F.col("_rn_exact") > 1, F.lit("EXACT"))
                       .when(F.col("_rn_client") > 1, F.lit("ANCIEN")))

    # Étape 3 : contrôles sur les lignes gardées.
    regles = [
        ("CLIENT_ID_INVALIDE", F.col("_id").isNull() | (F.col("_id") == "")),
        ("DATE_INVALIDE", F.col("_maj").isNull()),
    ]
    motifs = F.when(corrompue, F.array(F.lit("FORMAT_INVALIDE"))).otherwise(
        F.filter(F.array(*[F.when(c, F.lit(n)) for n, c in regles]), lambda m: m.isNotNull()))

    # Étape 4 : pseudonymisation (le sel vient d'un DataFrame, pas d'un littéral).
    df = df.crossJoin(F.broadcast(sel))
    return (df.withColumn("motifs", motifs)
            .withColumn("_email_ok", F.col("_email").rlike(FORME_EMAIL))
            .withColumn("_email_hash", F.when(F.col("_email_ok"),
                                              email_hache(F.col("_sel"), F.col("_email"))))
            .drop("_sel", "_rn_exact", "_rn_client"))


def separer_clients(evalue: DataFrame, run_id: str):
    """Renvoie (propres, rejets, doublons). Aucune donnée personnelle en clair dans les sorties."""
    gardees = evalue.filter(F.col("_doublon").isNull())
    propres = gardees.filter(F.size("motifs") == 0).select(
        F.col("_id").alias("customer_id"), F.col("_societe").alias("company_name"),
        F.col("_email_hash").alias("email_hash"), F.col("_pays").alias("country"),
        F.col("_ville").alias("city"), F.col("_segment").alias("segment"),
        F.col("_cree").alias("created_at"), F.col("_maj").alias("updated_at"), "livraison")
    propres = propres.select(
        *[F.col(f.name).cast(f.dataType).alias(f.name) for f in SILVER_CLIENTS_PSEUDO.fields],
        "livraison")

    # Les rejets gardent de quoi comprendre le motif, sans identifier la personne.
    rejets = gardees.filter(F.size("motifs") > 0).select(
        F.lit("clients").alias("source"), "livraison", "motifs",
        F.to_json(F.struct("customer_id", "company_name", "country", "city", "segment",
                           "created_at", "updated_at")).alias("ligne_origine"),
        F.lit(run_id).alias("run_id"), F.current_timestamp().alias("rejete_le"))
    return propres, rejets, evalue.filter(F.col("_doublon").isNotNull())
