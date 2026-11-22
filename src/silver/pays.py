"""Normalisation des pays (ticket BG-30).

Règle du projet : trim + upper + table de correspondance. Un pays inconnu ou vide
donne null, et c'est quality.py qui décide du rejet (motif PAYS_INVALIDE).
Les synonymes ci-dessous sont ceux réellement observés dans l'échantillon J1.
"""
from pyspark.sql import Column
from pyspark.sql import functions as F

# code ISO à 2 lettres -> toutes les écritures rencontrées (en majuscules)
SYNONYMES = {
    "FR": ["FR", "FRA", "FRANCE"],
    "DE": ["DE", "DEU", "DEUTSCHLAND"],
    "ES": ["ES", "ESP", "ESPAÑA"],
    "IT": ["IT", "ITA", "ITALIA"],
    "GB": ["GB", "GBR", "UNITED KINGDOM"],
    "US": ["US", "USA"],
    "PL": ["PL", "POL", "POLSKA"],
    "CN": ["CN", "CHN", "CHINA"],
    "BE": ["BE", "BEL", "BELGIQUE"],
    "NL": ["NL", "NLD", "NEDERLAND"],
}


def normaliser_pays(col: Column) -> Column:
    """' fr ' -> 'FR', 'Deutschland' -> 'DE', 'Atlantide' ou '' -> null."""
    paires = []
    for iso, ecritures in SYNONYMES.items():
        for ecriture in ecritures:
            paires += [F.lit(ecriture), F.lit(iso)]
    table = F.create_map(*paires)
    return table[F.upper(F.trim(col))]
