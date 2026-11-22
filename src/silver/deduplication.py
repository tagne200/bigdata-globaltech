"""Déduplication des commandes et règle des statuts (ticket BG-31).

Deux sortes de doublons, comptés à part pour le bilan "propres + rejets + doublons = lues" :

  EXACT   toutes les colonnes métier sont identiques à une autre ligne.
          On n'en garde qu'une, les autres sont des doublons.
  STATUT  même order_id, le contenu ne diffère que par le statut.
          On garde le statut le plus avancé (SHIPPED < COMPLETED < RETURNED < CANCELLED)
          avec row_number(), les autres lignes sont des doublons.

Les lignes mal formées (_corrupt_record) ne sont jamais comparées : leurs colonnes sont
peu fiables, elles vont directement en rejet (FORMAT_INVALIDE) dans quality.py.
"""
from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

COLONNES_METIER = [
    "order_id", "order_ts", "customer_id", "product_id", "quantity",
    "unit_price_eur", "country", "warehouse_id", "channel", "status",
]

# Plus le rang est grand, plus le statut est avancé dans la vie d'une commande.
RANG_STATUT = {"SHIPPED": 1, "COMPLETED": 2, "RETURNED": 3, "CANCELLED": 4}


def statut_normalise(col: Column) -> Column:
    """' completed ' -> 'COMPLETED'."""
    return F.upper(F.trim(col))


def rang_statut(col: Column) -> Column:
    """Rang du statut (1 à 4), ou 0 si le statut est vide ou inconnu."""
    paires = [x for statut, rang in RANG_STATUT.items() for x in (F.lit(statut), F.lit(rang))]
    return F.coalesce(F.create_map(*paires)[statut_normalise(col)], F.lit(0))


def marquer_doublons(commandes: DataFrame) -> DataFrame:
    """Ajoute la colonne `_doublon` : null (ligne à garder), 'EXACT' ou 'STATUT'.

    Aucune ligne n'est supprimée ici : on les marque, pour pouvoir les compter.
    """
    corrompue = F.col("_corrupt_record").isNotNull() if "_corrupt_record" in commandes.columns \
        else F.lit(False)
    df = commandes.withColumn("_rid", F.monotonically_increasing_id().cast("string"))

    # Étape 1 : doublons exacts. Une ligne corrompue a sa propre clé : jamais comparée.
    contenu = F.concat_ws("\u0001", *[F.coalesce(F.col(c), F.lit("<NULL>")) for c in COLONNES_METIER])
    cle_exacte = F.when(corrompue, F.concat(F.lit("corrompue-"), F.col("_rid"))) \
        .otherwise(F.sha2(contenu, 256))
    df = df.withColumn("_rn_exact", F.row_number().over(
        Window.partitionBy(cle_exacte).orderBy(F.col("_rid"))))

    # Étape 2 : même order_id, statut différent. Seules les lignes encore en jeu comptent :
    # les autres (doublons exacts, corrompues) ont leur propre clé pour ne rien fausser.
    en_jeu = (F.col("_rn_exact") == 1) & ~corrompue
    cle_statut = F.when(en_jeu, F.concat(F.lit("o-"), F.col("order_id"))) \
        .otherwise(F.concat(F.lit("x-"), F.col("_rid")))
    df = df.withColumn("_rn_statut", F.row_number().over(
        Window.partitionBy(cle_statut)
        .orderBy(rang_statut(F.col("status")).desc(), F.col("_rid"))))

    return (df.withColumn("_doublon", F.when(F.col("_rn_exact") > 1, "EXACT")
                          .when(F.col("_rn_statut") > 1, "STATUT"))
            .drop("_rid", "_rn_exact", "_rn_statut"))