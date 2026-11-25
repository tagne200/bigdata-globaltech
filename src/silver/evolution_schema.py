
"""BG-35 : detection et alignement des changements de schema Silver.

Fonctions natives Spark uniquement, sans UDF Python.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


# Colonnes techniques conservees pour la tracabilite.
COLONNES_TECHNIQUES = (
    "livraison",
    "ingest_date",
    "_source_file",
    "_ingest_ts",
    "_corrupt_record",
)


# Dates logiques des partitions Bronze.
DATES_LIVRAISONS = {
    "J1": "2026-02-01",
    "J2": "2026-03-01",
    "J3": "2026-04-01",
}


def lire_livraison(spark, chemin, livraison):
    """Lit uniquement la partition Bronze de la livraison demandee."""

    if livraison not in DATES_LIVRAISONS:
        raise ValueError(f"Livraison inconnue : {livraison}")

    date_ingestion = DATES_LIVRAISONS[livraison]

    chemin_partition = (
        f"{chemin}/ingest_date={date_ingestion}"
    )

    # basePath conserve la colonne de partition ingest_date.
    # On ne fusionne pas les schemas des autres livraisons.
    df = (
        spark.read
        .option("basePath", chemin)
        .parquet(chemin_partition)
    )

    return df.filter(F.col("livraison") == livraison)


def aligner_schema(
    df: DataFrame,
    colonnes_attendues,
    alias=None,
    obligatoires=("order_id",),
):
    """Compare le schema recu au schema cible et aligne les colonnes.

    Retourne :
      - le DataFrame aligne ;
      - un dictionnaire de differences de schema.
    """

    alias = alias or {}

    def normaliser(nom):
        return nom.strip().casefold()

    # Les alias explicites associent nom recu -> nom cible.
    alias_normalises = {
        normaliser(source): normaliser(cible)
        for source, cible in alias.items()
    }

    colonnes_recues = list(df.columns)
    techniques = [
        c for c in COLONNES_TECHNIQUES
        if c in colonnes_recues
    ]

    donnees_recues = [
        c for c in colonnes_recues
        if c not in COLONNES_TECHNIQUES
    ]

    correspondances = {}
    utilisees = set()
    renommees = []

    for cible in colonnes_attendues:
        # Priorite au nom exact.
        candidates = [
            source for source in donnees_recues
            if source == cible
        ]

        # Puis casse, espaces et alias explicites.
        if not candidates:
            candidates = [
                source for source in donnees_recues
                if (
                    normaliser(source) == normaliser(cible)
                    or alias_normalises.get(
                        normaliser(source)
                    ) == normaliser(cible)
                )
            ]

        if len(candidates) > 1:
            raise ValueError(
                f"BG-35 : colonnes ambiguës pour {cible} : "
                f"{candidates}"
            )

        if candidates:
            source = candidates[0]

            if source in utilisees:
                raise ValueError(
                    f"BG-35 : colonne utilisee deux fois : {source}"
                )

            correspondances[cible] = source
            utilisees.add(source)

            if source != cible:
                renommees.append(f"{source}->{cible}")

    # Une cle indispensable ne doit jamais etre inventee.
    manquantes_obligatoires = [
        c for c in obligatoires
        if c not in correspondances
    ]

    if manquantes_obligatoires:
        raise ValueError(
            "BG-35 : colonne obligatoire absente : "
            f"{manquantes_obligatoires}. "
            f"Colonnes recues : {colonnes_recues}"
        )

    ajoutees = [
        c for c in donnees_recues
        if c not in utilisees
    ]

    retirees = [
        c for c in colonnes_attendues
        if c not in correspondances
    ]

    def colonne(nom):
        # Protege les noms comportant des espaces ou caracteres speciaux.
        return F.col("`" + nom.replace("`", "``") + "`")

    expressions = []

    for cible in colonnes_attendues:
        if cible in correspondances:
            expressions.append(
                colonne(correspondances[cible]).alias(cible)
            )
        else:
            # Bronze stocke les donnees metier en texte.
            expressions.append(
                F.lit(None).cast("string").alias(cible)
            )

    # Conserver les colonnes de tracabilite existantes.
    expressions.extend(colonne(c) for c in techniques)

    df_aligne = df.select(*expressions)

    differences = {
        "colonnes_ajoutees": ajoutees,
        "colonnes_retirees": retirees,
        "colonnes_renommees": renommees,
    }

    return df_aligne, differences
