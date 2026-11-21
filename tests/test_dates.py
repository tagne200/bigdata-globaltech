import pytest
from pyspark.sql import functions as F

from src.silver.dates import parse_date, parse_order_ts


def _parse(spark, value):
    """Renvoie l'heure telle que Spark la voit (fuseau Europe/Paris), en texte.

    On formate côté Spark avec date_format : si on récupérait un datetime Python,
    il serait affiché dans le fuseau de la machine (UTC en CI) et fausserait le test.
    """
    df = spark.createDataFrame([(value,)], "raw string")
    return df.select(
        F.date_format(parse_order_ts(F.col("raw")), "yyyy-MM-dd HH:mm:ss").alias("ts")
    ).first()["ts"]


@pytest.mark.parametrize("raw", [
    "2026-01-13T14:16:49",      # ISO
    "2026-01-13 14:16:49",      # espace au lieu de T
    "13/01/2026 14:16:49",      # format français
    "  2026-01-13T14:16:49 ",   # espaces autour
])
def test_formats_texte_donnent_le_meme_instant(spark, raw):
    assert _parse(spark, raw) == "2026-01-13 14:16:49"


def test_epoch_ms_est_converti_en_heure_de_paris(spark):
    # 1768310209000 ms = 2026-01-13 13:16:49 UTC = 14:16:49 à Paris (UTC+1 en hiver)
    assert _parse(spark, "1768310209000") == "2026-01-13 14:16:49"


@pytest.mark.parametrize("raw", ["N/A", "hier", "", "   ", "2026-01", "2026", "abc", None,
                                 "2026-13-45 99:99:99"])
def test_valeurs_inutilisables_donnent_null(spark, raw):
    assert _parse(spark, raw) is None


def test_parse_date_simple(spark):
    df = spark.createDataFrame([("2025-12-16",), ("16/12/2025",), ("",)], "raw string")
    out = [r["d"] for r in df.select(parse_date(F.col("raw")).alias("d")).collect()]
    assert str(out[0]) == "2025-12-16" and out[1] is None and out[2] is None
