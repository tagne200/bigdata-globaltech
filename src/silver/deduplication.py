
"""Déduplication des commandes Silver — ticket BG-31."""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def marquer_doublons(propres: DataFrame) -> DataFrame:
    """Classe les commandes par order_id et identifie les doublons.

    Priorité des statuts :
    CANCELLED > RETURNED > COMPLETED > SHIPPED.

    Un seul enregistrement est conservé par order_id.
    """

    # 1. Vérifier la présence d'un identifiant exploitable.
    identifiant_invalide = (
        F.col("order_id").isNull()
        | (F.trim(F.col("order_id")) == "")
    )

    if propres.filter(identifiant_invalide).limit(1).count() > 0:
        raise ValueError(
            "BG-31 : order_id manquant dans les commandes propres. "
            "La qualité de cet identifiant doit être contrôlée."
        )

    # 2. Définir la priorité métier de chaque statut.
    statut = F.upper(F.trim(F.col("status")))

    priorite = (
        F.when(statut == "CANCELLED", 4)
        .when(statut == "RETURNED", 3)
        .when(statut == "COMPLETED", 2)
        .when(statut == "SHIPPED", 1)
        .otherwise(0)
    )

    # 3. Définir un départage stable en cas d'égalité.
    autres_colonnes = sorted(
        c for c in propres.columns
        if c not in {"order_id", "order_ts"}
    )

    fenetre = Window.partitionBy("order_id").orderBy(
        F.col("_priorite").desc(),
        F.col("order_ts").desc_nulls_last(),
        *[
            F.col(c).asc_nulls_last()
            for c in autres_colonnes
        ],
    )

    # 4. Attribuer un numéro à chaque ligne du même order_id.
    classes = (
        propres
        .withColumn("_priorite", priorite)
        .withColumn("_rang_doublon", F.row_number().over(fenetre))
        .drop("_priorite")
    )

    # 5. La première ligne est conservée, les suivantes sont des doublons.
    return classes.withColumn(
        "est_doublon",
        F.col("_rang_doublon") > 1,
    )
