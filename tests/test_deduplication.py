
"""Tests de déduplication des commandes — BG-31."""

from datetime import datetime

import pytest

from src.silver.deduplication import marquer_doublons


DATE_1 = datetime(2026, 1, 10, 10, 0)
DATE_2 = datetime(2026, 1, 15, 10, 0)


def evaluer(spark, lignes):
    """Construit des commandes et applique la déduplication."""
    df = spark.createDataFrame(
        lignes,
        schema=(
            "order_id string, order_ts timestamp, "
            "status string, warehouse_id string"
        ),
    )
    return marquer_doublons(df).collect()


def test_commandes_differentes(spark):
    resultats = evaluer(spark, [
        ("CMD001", DATE_1, "SHIPPED", "W1"),
        ("CMD002", DATE_1, "COMPLETED", "W1"),
    ])

    assert len(resultats) == 2
    assert all(not r["est_doublon"] for r in resultats)


def test_doublons_exacts(spark):
    resultats = evaluer(spark, [
        ("CMD001", DATE_1, "SHIPPED", "W1"),
        ("CMD001", DATE_1, "SHIPPED", "W1"),
    ])

    assert sum(r["est_doublon"] for r in resultats) == 1
    assert sum(not r["est_doublon"] for r in resultats) == 1


def test_priorite_des_statuts(spark):
    resultats = evaluer(spark, [
        ("CMD001", DATE_1, "SHIPPED", "W1"),
        ("CMD001", DATE_1, "COMPLETED", "W1"),
        ("CMD001", DATE_1, "RETURNED", "W1"),
        ("CMD001", DATE_1, "CANCELLED", "W1"),
    ])

    conservees = [r for r in resultats if not r["est_doublon"]]

    assert len(conservees) == 1
    assert conservees[0]["status"] == "CANCELLED"
    assert sum(r["est_doublon"] for r in resultats) == 3


def test_date_la_plus_recente_en_cas_egalite(spark):
    resultats = evaluer(spark, [
        ("CMD001", DATE_1, "COMPLETED", "W1"),
        ("CMD001", DATE_2, "COMPLETED", "W2"),
    ])

    conservee = next(r for r in resultats if not r["est_doublon"])

    assert conservee["warehouse_id"] == "W2"


def test_departage_stable(spark):
    resultats = evaluer(spark, [
        ("CMD001", DATE_1, "COMPLETED", "W2"),
        ("CMD001", DATE_1, "COMPLETED", "W1"),
    ])

    conservee = next(r for r in resultats if not r["est_doublon"])

    assert conservee["warehouse_id"] == "W1"


def test_identifiant_manquant(spark):
    with pytest.raises(ValueError, match="order_id manquant"):
        evaluer(spark, [
            ("", DATE_1, "SHIPPED", "W1"),
        ])
