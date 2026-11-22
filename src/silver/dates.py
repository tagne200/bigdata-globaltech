"""Parsing des dates multi-formats avec des fonctions natives Spark (ticket BG-29).

Les données contiennent 4 formats pour order_ts (mesurés sur l'échantillon J1) :
    2026-01-13T14:16:49     ISO (100 900 lignes)
    1768313809000           epoch en millisecondes (82)
    2026-01-13 14:16:49     (76)
    13/01/2026 14:16:49     (59)
et des valeurs inutilisables ("N/A", "hier", vide...) qui doivent donner null.

Principe : on essaie chaque format, et coalesce() garde le premier résultat non nul.
Chaque essai est protégé par un rlike() : on n'applique un format que si la
chaîne "a la bonne forme". Pas d'UDF Python : tout est exécuté par le moteur Spark.
"""
from pyspark.sql import Column
from pyspark.sql import functions as F

# (expression régulière de la forme attendue, format Spark correspondant)
_STRING_FORMATS = [
    (r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$", "yyyy-MM-dd'T'HH:mm:ss"),
    (r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$", "yyyy-MM-dd HH:mm:ss"),
    (r"^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}$", "dd/MM/yyyy HH:mm:ss"),
]
_EPOCH_MS = r"^\d{12,13}$"  # 12-13 chiffres = millisecondes depuis 1970


def parse_order_ts(col: Column) -> Column:
    """Convertit une colonne texte en timestamp, ou null si aucun format ne convient.

    Les chaînes (sans fuseau) sont lues en heure de Paris : c'est le fuseau de
    session `spark.sql.session.timeZone` (voir docker/spark-defaults.conf).
    L'epoch, lui, désigne un instant absolu (UTC) : pas d'ambiguïté.
    """
    s = F.trim(col)
    candidates = [
        F.when(s.rlike(pattern), F.to_timestamp(s, fmt))
        for pattern, fmt in _STRING_FORMATS
    ]
    # epoch ms -> secondes (division par 1000) -> timestamp
    candidates.append(F.when(s.rlike(_EPOCH_MS), (s.cast("long") / 1000).cast("timestamp")))
    return F.coalesce(*candidates)


def parse_date(col: Column) -> Column:
    """Date simple 'yyyy-MM-dd' (updated_at des produits, created_at des clients)."""
    s = F.trim(col)
    return F.when(s.rlike(r"^\d{4}-\d{2}-\d{2}$"), F.to_date(s, "yyyy-MM-dd"))

def parse_date_multi(col: Column) -> Column:
    """Date simple en 'yyyy-MM-dd' ou 'dd/MM/yyyy' (created_at des clients), sinon null."""
    s = F.trim(col)
    return F.coalesce(
        F.when(s.rlike(r"^\d{4}-\d{2}-\d{2}$"), F.to_date(s, "yyyy-MM-dd")),
        F.when(s.rlike(r"^\d{2}/\d{2}/\d{4}$"), F.to_date(s, "dd/MM/yyyy")),
    )
