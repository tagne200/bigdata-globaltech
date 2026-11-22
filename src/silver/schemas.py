"""Schémas explicites des sources (ticket BG-29).

Pourquoi explicites ? Avec inferSchema, Spark relit les données pour deviner les
types : c'est lent, et un seul "N/A" dans une colonne numérique la rend "string".
On déclare donc nous-mêmes ce qu'on attend.

Deux familles de schémas :
  - RAW_*    : tout en texte (StringType). C'est ce qu'on lit depuis le CSV / la bronze.
               Lire en texte garantit qu'aucune ligne n'est perdue à la lecture.
  - SILVER_* : les types cibles, après nettoyage. La conversion texte -> type
               est faite par dates.py et nombres.py.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType, DecimalType, IntegerType, StringType, StructField, StructType,
    TimestampType,
)


def _all_strings(*names):
    """Construit un StructType où chaque colonne est du texte, et nullable."""
    return StructType([StructField(n, StringType(), True) for n in names])


# ---------- Lecture brute : tout en texte ----------
RAW_COMMANDES = _all_strings(
    "order_id", "order_ts", "customer_id", "product_id", "quantity",
    "unit_price_eur", "country", "warehouse_id", "channel", "status",
)
RAW_PRODUITS = _all_strings(
    "product_id", "product_name", "family", "category",
    "unit_price_eur", "updated_at", "extracted_at",
)
RAW_CLIENTS = _all_strings(
    "customer_id", "company_name", "contact_first_name", "contact_last_name",
    "email", "phone", "country", "city", "segment", "created_at", "updated_at",
)

# Les montants sont en DECIMAL(12,2) et non en double : un double fait des
# erreurs d'arrondi binaires (0.1 + 0.2 != 0.3), inacceptable pour un chiffre d'affaires.
PRICE_TYPE = DecimalType(12, 2)

# ---------- Types cibles de la silver ----------
SILVER_COMMANDES = StructType([
    StructField("order_id", StringType(), False),
    StructField("order_ts", TimestampType(), True),   # null si la date est inutilisable (BG-30 décidera du rejet)
    StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price_eur", PRICE_TYPE, True),
    StructField("country", StringType(), True),
    StructField("warehouse_id", StringType(), True),
    StructField("channel", StringType(), True),
    StructField("status", StringType(), True),
])
SILVER_PRODUITS = StructType([
    StructField("product_id", StringType(), False),
    StructField("product_name", StringType(), True),
    StructField("family", StringType(), True),
    StructField("category", StringType(), True),
    StructField("unit_price_eur", PRICE_TYPE, True),
    StructField("updated_at", DateType(), True),
    StructField("extracted_at", TimestampType(), True),
])
SILVER_CLIENTS = StructType([
    StructField("customer_id", StringType(), False),
    StructField("company_name", StringType(), True),
    StructField("contact_first_name", StringType(), True),
    StructField("contact_last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("country", StringType(), True),
    StructField("city", StringType(), True),
    StructField("segment", StringType(), True),
    StructField("created_at", DateType(), True),
    StructField("updated_at", TimestampType(), True),
])


def imposer_schema(df: DataFrame, schema: StructType, colonnes_en_plus=()) -> DataFrame:
    """Force un DataFrame à respecter un schéma : mêmes colonnes, même ordre, mêmes types.

    On sélectionne chaque colonne du schéma et on la convertit vers son type cible.
    Les colonnes techniques à conserver (livraison, ingest_date...) se passent à part.
    """
    colonnes = [F.col(f.name).cast(f.dataType).alias(f.name) for f in schema.fields]
    return df.select(*colonnes, *colonnes_en_plus)