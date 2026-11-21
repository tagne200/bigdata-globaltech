from decimal import Decimal

import pytest
from pyspark.sql import functions as F

from src.silver.numbers import parse_price


def _parse(spark, value):
    df = spark.createDataFrame([(value,)], "raw string")
    return df.select(parse_price(F.col("raw")).alias("p")).first()["p"]


@pytest.mark.parametrize("raw, attendu", [
    ("205,16", Decimal("205.16")),    # virgule décimale : le cas du ticket
    ("83.34", Decimal("83.34")),      # point : cas normal
    ("  54.76 ", Decimal("54.76")),   # espaces
    ("12", Decimal("12.00")),         # entier
    ("-34.69", Decimal("-34.69")),    # négatif : converti, rejeté plus tard (BG-30)
])
def test_prix_valides(spark, raw, attendu):
    assert _parse(spark, raw) == attendu


@pytest.mark.parametrize("raw", ["", "abc", "12,5,3", "1 200", None])
def test_prix_invalides_donnent_null(spark, raw):
    assert _parse(spark, raw) is None
