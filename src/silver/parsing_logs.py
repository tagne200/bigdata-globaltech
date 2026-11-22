
"""BG-34 : parsing des logs Apache Combined avec les fonctions natives Spark."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from src.silver.dates import parse_order_ts


# Expression régulière correspondant au format Apache Combined
# étendu par une durée de réponse en fin de ligne.
LOG_REGEX = (
    r'^(\S+) \S+ \S+ \[([^\]]+)\] '
    r'"(\S+) (\S+) (HTTP/[0-9.]+)" '
    r'(\d{3}) (\d+|-)\s+'
    r'"([^"]*)" "([^"]*)" (\d+)\s*$'
)


def parser_logs(df: DataFrame) -> DataFrame:
    """Transforme les lignes Bronze en logs structurés et identifie les rejets."""

    if "ligne" not in df.columns:
        raise ValueError("BG-34 : colonne 'ligne' absente des logs Bronze")

    # Vérifier si la ligne correspond au format attendu.
    resultat = df.withColumn(
        "_match",
        F.regexp_extract(F.col("ligne"), LOG_REGEX, 0)
    )

    # Extraire les champs à l'aide des groupes de la regex.
    champs = {
        "ip": 1,
        "date_brute": 2,
        "methode": 3,
        "url": 4,
        "protocole": 5,
        "statut_brut": 6,
        "taille_brute": 7,
        "referer": 8,
        "user_agent": 9,
        "temps_reponse_brut": 10,
    }

    for nom, numero in champs.items():
        resultat = resultat.withColumn(
            nom,
            F.regexp_extract(F.col("ligne"), LOG_REGEX, numero)
        )

    # Convertir les valeurs textuelles vers leurs types Spark.
    resultat = (
        resultat
        .withColumn(
            "horodatage",
            # Profilage J1, section 4 : 58 logs ont une date yyyy-MM-dd HH:mm:ss.
            # On les convertit avec le fuseau Europe/Paris configuré dans Spark.
            F.coalesce(
                F.to_timestamp(
                    "dd/MMM/yyyy:HH:mm:ss Z"
                    ),
                parse_order_ts(F.col("date_brute"))
                )
        )
        .withColumn(
            "statut_http",
            F.col("statut_brut").cast("int")
        )
        .withColumn(
            "taille_octets",
            F.when(
                F.col("taille_brute") != "-",
                F.col("taille_brute").cast("long")
            )
        )
        .withColumn(
            "temps_reponse",
            F.col("temps_reponse_brut").cast("long")
        )
    )

    # Identifier les lignes à rejeter.
    resultat = resultat.withColumn(
        "motif_rejet",
        F.when(
            F.col("_match").isNull() | (F.col("_match") == ""),
            F.lit("FORMAT_INVALIDE")
        )
        .when(
            F.col("horodatage").isNull(),
            F.lit("DATE_INVALIDE")
        )
        .when(
            ~F.col("statut_http").between(100, 599),
            F.lit("STATUT_HTTP_INVALIDE")
        )
    )

    # Conserver les informations techniques de traçabilité.
    colonnes_techniques = [
        c for c in ("livraison", "ingest_date", "_source_file", "_ingest_ts")
        if c in resultat.columns
    ]

    return resultat.select(
        "ligne",
        "ip",
        "horodatage",
        "methode",
        "url",
        "protocole",
        "statut_http",
        "taille_octets",
        "referer",
        "user_agent",
        "temps_reponse",
        "motif_rejet",
        *colonnes_techniques,
    )
