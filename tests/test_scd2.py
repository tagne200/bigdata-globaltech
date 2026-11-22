from datetime import date
from decimal import Decimal

import pytest
from pyspark.sql import functions as F

from src.silver.scd2 import evaluer_produits, normaliser_famille, separer_produits

DDL = ("product_id string, product_name string, family string, category string, "
       "unit_price_eur string, updated_at string, extracted_at string, "
       "_corrupt_record string, livraison string")
COLS = [c.split()[0] for c in DDL.split(", ")]
BASE = dict(product_id="P1", product_name="Relais", family="Câblage", category="Relais",
            unit_price_eur="10.00", updated_at="2025-10-01", extracted_at="2026-02-01T02:00:00",
            _corrupt_record=None, livraison="J1")
FIN = date(9999, 12, 31)


def _historiser(spark, *lignes):
    rows = [tuple({**BASE, **l}[c] for c in COLS) for l in lignes]
    bronze = spark.createDataFrame(rows, DDL)
    return separer_produits(evaluer_produits(bronze), "run1"), len(rows)


def _versions(spark, *lignes):
    (versions, _, _), _ = _historiser(spark, *lignes)
    return [(r["product_id"], r["valid_from"], r["valid_to"], r["is_current"],
             r["unit_price_eur"]) for r in versions.orderBy("product_id", "valid_from").collect()]


def test_une_seule_version_est_courante_jusqu_a_la_fin_des_temps(spark):
    assert _versions(spark, {}) == [("P1", date(2025, 10, 1), FIN, True, Decimal("10.00"))]


def test_la_version_precedente_se_termine_au_debut_de_la_suivante(spark):
    v = _versions(spark, {}, {"updated_at": "2025-12-30", "unit_price_eur": "12.00"})

    assert v == [
        ("P1", date(2025, 10, 1), date(2025, 12, 30), False, Decimal("10.00")),
        ("P1", date(2025, 12, 30), FIN, True, Decimal("12.00")),
    ]


def test_trois_versions_une_seule_courante(spark):
    v = _versions(spark, {}, {"updated_at": "2025-11-01", "unit_price_eur": "11.00"},
                  {"updated_at": "2025-12-01", "unit_price_eur": "12.00"})
    assert [x[3] for x in v] == [False, False, True]
    assert [x[2] for x in v] == [date(2025, 11, 1), date(2025, 12, 1), FIN]


def test_l_ordre_des_lignes_en_entree_ne_change_rien(spark):
    a = _versions(spark, {}, {"updated_at": "2025-12-30", "unit_price_eur": "12.00"})
    b = _versions(spark, {"updated_at": "2025-12-30", "unit_price_eur": "12.00"}, {})
    assert a == b


def test_plusieurs_produits_sont_historises_separement(spark):
    v = _versions(spark, {}, {"product_id": "P2"}, {"updated_at": "2025-12-30"})
    assert sorted((p, c) for p, _, _, c, _ in v) == [("P1", False), ("P1", True), ("P2", True)]


def test_meme_version_en_double_on_garde_l_extraction_la_plus_recente(spark):
    (versions, _, doublons), _ = _historiser(
        spark, {"unit_price_eur": "10.00", "extracted_at": "2026-02-01T02:00:00"},
        {"unit_price_eur": "99.00", "extracted_at": "2026-02-01T03:00:00"})
    assert [r["unit_price_eur"] for r in versions.collect()] == [Decimal("99.00")]
    assert doublons.count() == 1


def test_version_deja_connue_renvoyee_dans_un_delta_ne_cree_pas_de_version(spark):
    v = _versions(spark, {}, {"livraison": "J2", "extracted_at": "2026-03-01T02:00:00"})
    assert len(v) == 1


def test_delta_nouvelle_version_ferme_l_ancienne(spark):
    v = _versions(spark, {}, {"livraison": "J2", "updated_at": "2026-02-15", "unit_price_eur": "14,50"})
    assert v[0][2] == date(2026, 2, 15) and v[0][3] is False
    assert v[1] == ("P1", date(2026, 2, 15), FIN, True, Decimal("14.50"))


@pytest.mark.parametrize("brut", ["  DISTRIBUTION ÉLECTRIQUE ", "distribution électrique",
                                  "Distribution électrique"])
def test_la_famille_est_normalisee(spark, brut):
    df = spark.createDataFrame([(brut,)], "f string")
    assert df.select(normaliser_famille(F.col("f")).alias("x")).first()["x"] == "Distribution Électrique"


@pytest.mark.parametrize("champ, valeur, motif", [
    ("unit_price_eur", "-34.69", "PRIX_INVALIDE"),
    ("unit_price_eur", "0", "PRIX_INVALIDE"),
    ("unit_price_eur", "abc", "PRIX_INVALIDE"),
    ("updated_at", "N/A", "DATE_INVALIDE"),
    ("updated_at", "13/01/2026", "DATE_INVALIDE"),
    ("product_id", "", "PRODUIT_ID_INVALIDE"),
    ("family", "  ", "FAMILLE_INVALIDE"),
])
def test_une_version_invalide_est_rejetee_avec_son_motif(spark, champ, valeur, motif):
    (versions, rejets, _), _ = _historiser(spark, {champ: valeur})
    assert versions.count() == 0
    assert rejets.first()["motifs"] == [motif]


def test_une_ligne_mal_formee_ne_garde_que_format_invalide(spark):
    (versions, rejets, _), _ = _historiser(spark, {"_corrupt_record": "P1,x", "updated_at": "N/A"})
    assert versions.count() == 0 and rejets.first()["motifs"] == ["FORMAT_INVALIDE"]


def test_une_version_rejetee_prolonge_la_version_precedente(spark):
    # la version du 2025-11-01 est rejetée (prix négatif) : la première reste courante
    v = _versions(spark, {}, {"updated_at": "2025-11-01", "unit_price_eur": "-5"})
    assert v == [("P1", date(2025, 10, 1), FIN, True, Decimal("10.00"))]


def test_chaque_ligne_lue_est_comptee_une_seule_fois(spark):
    (versions, rejets, doublons), lues = _historiser(
        spark, {}, {}, {"updated_at": "2025-12-01"}, {"unit_price_eur": "-1", "updated_at": "2025-11-01"},
        {"_corrupt_record": "x"}, {"product_id": "P2"})
    assert versions.count() + rejets.count() + doublons.count() == lues == 6
    assert (versions.count(), rejets.count(), doublons.count()) == (3, 2, 1)


def test_jointure_temporelle_valid_to_exclue(spark):
    from pyspark.sql import functions as F

    # Deux versions du même produit, avec deux prix différents.
    (versions, _, _), _ = _historiser(
        spark,
        {"updated_at": "2025-11-01", "unit_price_eur": "11.00"},
        {"updated_at": "2025-12-01", "unit_price_eur": "12.00"},
    )

    # Deux commandes autour du changement de prix.
    commandes = spark.createDataFrame(
        [
            ("O1", "2025-11-30 14:00:00"),
            ("O2", "2025-12-01 08:00:00"),
        ],
        "order_id string, order_ts string",
    ).withColumn("order_ts", F.to_timestamp("order_ts"))

    # Jointure temporelle avec une date de fin EXCLUE.
    resultat = commandes.join(
        versions,
        (F.col("order_ts") >= F.col("valid_from").cast("timestamp"))
        & (F.col("order_ts") < F.col("valid_to").cast("timestamp")),
        "inner",
    ).select("order_id", "unit_price_eur").collect()

    # Chaque commande doit retrouver exactement un prix.
    assert len(resultat) == 2

    prix = {
        ligne["order_id"]: float(ligne["unit_price_eur"])
        for ligne in resultat
    }

    assert prix == {
        "O1": 11.0,
        "O2": 12.0,
    }