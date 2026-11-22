import pytest
from pyspark.sql import functions as F

from src.silver.deduplication import marquer_doublons, rang_statut

DDL = ("order_id string, order_ts string, customer_id string, product_id string, "
       "quantity string, unit_price_eur string, country string, warehouse_id string, "
       "channel string, status string, _corrupt_record string")
COLS = [c.split()[0] for c in DDL.split(", ")]
BASE = dict(order_id="O1", order_ts="2026-01-13T14:16:49", customer_id="C1", product_id="P1",
            quantity="2", unit_price_eur="10.50", country="FR", warehouse_id="W1",
            channel="web", status="COMPLETED", _corrupt_record=None)


def _marquer(spark, *lignes):
    rows = [tuple({**BASE, **l}[c] for c in COLS) for l in lignes]
    df = marquer_doublons(spark.createDataFrame(rows, DDL))
    return [(r["order_id"], r["status"], r["_doublon"]) for r in df.collect()]


def test_ligne_unique_est_gardee(spark):
    assert _marquer(spark, {}) == [("O1", "COMPLETED", None)]


def test_doublon_exact_une_seule_ligne_gardee(spark):
    res = _marquer(spark, {}, {}, {})
    assert sorted(d or "GARDEE" for _, _, d in res) == ["EXACT", "EXACT", "GARDEE"]


@pytest.mark.parametrize("statuts, gardé", [
    (["COMPLETED", "CANCELLED"], "CANCELLED"),
    (["SHIPPED", "COMPLETED"], "COMPLETED"),
    (["COMPLETED", "RETURNED"], "RETURNED"),
    (["RETURNED", "CANCELLED"], "CANCELLED"),
    (["SHIPPED", "CANCELLED"], "CANCELLED"),
])
def test_on_garde_le_statut_le_plus_avance(spark, statuts, gardé):
    res = _marquer(spark, *[{"status": s} for s in statuts])
    assert [s for _, s, d in res if d is None] == [gardé]
    assert [d for _, s, d in res if s != gardé] == ["STATUT"]


def test_le_resultat_ne_depend_pas_de_l_ordre_des_lignes(spark):
    a = _marquer(spark, {"status": "SHIPPED"}, {"status": "RETURNED"})
    b = _marquer(spark, {"status": "RETURNED"}, {"status": "SHIPPED"})
    assert [s for _, s, d in a if d is None] == [s for _, s, d in b if d is None] == ["RETURNED"]


def test_la_casse_et_les_espaces_du_statut_ne_comptent_pas(spark):
    res = _marquer(spark, {"status": " completed "}, {"status": "CANCELLED"})
    assert [s for _, s, d in res if d is None] == ["CANCELLED"]


def test_un_statut_valide_l_emporte_sur_un_statut_vide(spark):
    res = _marquer(spark, {"status": ""}, {"status": "SHIPPED"})
    assert [s for _, s, d in res if d is None] == ["SHIPPED"]


def test_order_id_differents_ne_sont_pas_des_doublons(spark):
    res = _marquer(spark, {"order_id": "O1"}, {"order_id": "O2"})
    assert all(d is None for _, _, d in res)


def test_lignes_mal_formees_ne_sont_jamais_des_doublons(spark):
    mal = {"_corrupt_record": "O1,x,y"}
    res = _marquer(spark, mal, mal, {})
    assert all(d is None for _, _, d in res)


def test_doublon_exact_puis_statut_dans_le_meme_groupe(spark):
    res = _marquer(spark, {"status": "SHIPPED"}, {"status": "SHIPPED"}, {"status": "CANCELLED"})
    assert sorted(d or "GARDEE" for _, _, d in res) == ["EXACT", "GARDEE", "STATUT"]


@pytest.mark.parametrize("brut, rang", [
    ("shipped", 1), (" COMPLETED ", 2), ("Returned", 3), ("CANCELLED", 4), ("", 0), ("pending", 0),
])
def test_rang_du_statut(spark, brut, rang):
    df = spark.createDataFrame([(brut,)], "s string")
    assert df.select(rang_statut(F.col("s")).alias("r")).first()["r"] == rang