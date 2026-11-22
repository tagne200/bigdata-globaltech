from decimal import Decimal

import pytest
from pyspark.sql import functions as F

from src.silver.pays import normaliser_pays
from src.silver.quality import evaluer_commandes, separer

DDL = ("order_id string, order_ts string, customer_id string, product_id string, "
       "quantity string, unit_price_eur string, country string, warehouse_id string, "
       "channel string, status string, _corrupt_record string, livraison string, "
       "ingest_date string")
VALIDE = dict(order_id="O1", order_ts="2026-01-13T14:16:49", customer_id="C1",
              product_id="P1", quantity="2", unit_price_eur="10,50", country="France",
              warehouse_id="W1", channel="web", status="COMPLETED",
              _corrupt_record=None, livraison="J1", ingest_date="2026-02-01")
DEBUT, FIN = "2026-01-01", "2026-02-01"


def _evaluer(spark, **changements):
    ligne = {**VALIDE, **changements}
    commandes = spark.createDataFrame([tuple(ligne[c] for c in DDL_COLS)], DDL)
    produits = spark.createDataFrame([("P1",)], "product_id string")
    clients = spark.createDataFrame([("C1",)], "customer_id string")
    return evaluer_commandes(commandes, produits, clients, DEBUT, FIN)


DDL_COLS = [c.split()[0] for c in DDL.split(", ")]


def _motifs(spark, **changements):
    return _evaluer(spark, **changements).first()["motifs"]


def test_ligne_valide_est_propre_et_convertie(spark):
    ligne = _evaluer(spark).first()
    assert ligne["motifs"] == []
    assert ligne["_qte"] == 2 and ligne["_prix"] == Decimal("10.50") and ligne["_pays"] == "FR"


@pytest.mark.parametrize("champ, valeur, motif", [
    ("order_ts", "N/A", "DATE_INVALIDE"),
    ("order_ts", "hier", "DATE_INVALIDE"),
    ("order_ts", "", "DATE_INVALIDE"),
    ("order_ts", "2025-12-29T08:31:14", "DATE_HORS_PERIODE"),
    ("order_ts", "2030-01-05T10:00:00", "DATE_HORS_PERIODE"),
    ("order_ts", "2026-02-01T00:00:00", "DATE_HORS_PERIODE"),   # la borne de fin est exclue
    ("country", "", "PAYS_INVALIDE"),
    ("country", "Atlantide", "PAYS_INVALIDE"),
    ("quantity", "abc", "QUANTITE_INVALIDE"),
    ("quantity", "2 pcs", "QUANTITE_INVALIDE"),
    ("quantity", "0", "QUANTITE_INVALIDE"),
    ("quantity", "-1", "QUANTITE_INVALIDE"),
    ("quantity", "", "QUANTITE_INVALIDE"),
    ("unit_price_eur", "-43.62", "PRIX_INVALIDE"),
    ("unit_price_eur", "0", "PRIX_INVALIDE"),
    ("unit_price_eur", "", "PRIX_INVALIDE"),
    ("product_id", "P999", "PRODUIT_ORPHELIN"),
    ("product_id", "", "PRODUIT_ORPHELIN"),
    ("customer_id", "C999", "CLIENT_ORPHELIN"),
    ("customer_id", "", "CLIENT_ORPHELIN"),
])
def test_un_defaut_donne_un_seul_motif(spark, champ, valeur, motif):
    assert _motifs(spark, **{champ: valeur}) == [motif]


def test_plusieurs_defauts_donnent_une_ligne_et_tous_les_motifs(spark):
    motifs = _motifs(spark, order_ts="N/A", quantity="abc", country="")
    assert sorted(motifs) == ["DATE_INVALIDE", "PAYS_INVALIDE", "QUANTITE_INVALIDE"]


def test_ligne_mal_formee_ne_garde_que_format_invalide(spark):
    # les autres colonnes d'une ligne corrompue sont fausses : on ne les juge pas
    motifs = _motifs(spark, _corrupt_record="ORD1,2026-01-13,,,", order_ts="N/A", quantity="abc")
    assert motifs == ["FORMAT_INVALIDE"]


def test_propres_plus_rejets_egale_lignes_lues(spark):
    lignes = [VALIDE, {**VALIDE, "order_id": "O2", "quantity": "abc"},
              {**VALIDE, "order_id": "O3", "country": "", "order_ts": "N/A"},
              {**VALIDE, "order_id": "O4", "_corrupt_record": "x"}]
    commandes = spark.createDataFrame([tuple(l[c] for c in DDL_COLS) for l in lignes], DDL)
    produits = spark.createDataFrame([("P1",)], "product_id string")
    clients = spark.createDataFrame([("C1",)], "customer_id string")
    propres, rejets = separer(evaluer_commandes(commandes, produits, clients, DEBUT, FIN), "run1")
    assert propres.count() == 1 and rejets.count() == 3
    assert propres.count() + rejets.count() == commandes.count()
    assert rejets.first()["run_id"] == "run1"
    # la ligne d'origine est conservée comme preuve du rejet
    assert rejets.filter(F.col("ligne_origine").contains('"order_id":"O2"')).count() == 1


@pytest.mark.parametrize("brut, attendu", [
    ("France", "FR"), (" fr ", "FR"), ("FRA", "FR"), ("Deutschland", "DE"),
    ("España", "ES"), ("united kingdom", "GB"), ("USA", "US"), ("", None), ("Atlantide", None),
])
def test_normalisation_des_pays(spark, brut, attendu):
    df = spark.createDataFrame([(brut,)], "raw string")
    assert df.select(normaliser_pays(F.col("raw")).alias("p")).first()["p"] == attendu


@pytest.mark.parametrize("brut, attendu", [
    ("completed", "COMPLETED"), (" Cancelled ", "CANCELLED"), ("SHIPPED", "SHIPPED"),
])
def test_le_statut_est_normalise_en_silver(spark, brut, attendu):
    ligne = {**VALIDE, "status": brut}
    commandes = spark.createDataFrame([tuple(ligne[c] for c in DDL_COLS)], DDL)
    produits = spark.createDataFrame([("P1",)], "product_id string")
    clients = spark.createDataFrame([("C1",)], "customer_id string")
    propres, _ = separer(evaluer_commandes(commandes, produits, clients, DEBUT, FIN), "run1")
    assert propres.first()["status"] == attendu