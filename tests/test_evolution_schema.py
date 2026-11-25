
"""Tests BG-35 : évolution du schéma des commandes."""

import pytest

from pyspark.sql.types import StringType

from src.silver.evolution_schema import (
    aligner_schema,
    lire_livraison,
)


def creer_df(spark, colonnes):
    """Crée une ligne de test avec des valeurs textuelles."""
    valeurs = tuple(f"valeur_{i}" for i in range(len(colonnes)))
    return spark.createDataFrame([valeurs], colonnes)


def test_01_schema_identique(spark):
    df = creer_df(spark, ["order_id", "channel"])
    resultat, ecarts = aligner_schema(df, ["order_id", "channel"])

    assert resultat.columns == ["order_id", "channel"]
    assert ecarts == {
        "colonnes_ajoutees": [],
        "colonnes_retirees": [],
        "colonnes_renommees": [],
    }


def test_02_colonne_ajoutee(spark):
    df = creer_df(spark, ["order_id", "channel", "discount_pct"])
    resultat, ecarts = aligner_schema(df, ["order_id", "channel"])

    assert ecarts["colonnes_ajoutees"] == ["discount_pct"]
    assert "discount_pct" not in resultat.columns


def test_03_colonne_retiree(spark):
    df = creer_df(spark, ["order_id"])
    resultat, ecarts = aligner_schema(df, ["order_id", "channel"])

    assert ecarts["colonnes_retirees"] == ["channel"]
    assert resultat.first()["channel"] is None
    assert isinstance(resultat.schema["channel"].dataType, StringType)


def test_04_colonne_ajoutee_et_retiree(spark):
    df = creer_df(spark, ["order_id", "discount_pct"])
    _, ecarts = aligner_schema(df, ["order_id", "channel"])

    assert ecarts["colonnes_ajoutees"] == ["discount_pct"]
    assert ecarts["colonnes_retirees"] == ["channel"]


def test_05_renommage_casse(spark):
    df = creer_df(spark, ["ORDER_ID", "CHANNEL"])
    resultat, ecarts = aligner_schema(df, ["order_id", "channel"])

    assert resultat.columns == ["order_id", "channel"]
    assert ecarts["colonnes_renommees"] == [
        "ORDER_ID->order_id",
        "CHANNEL->channel",
    ]


def test_06_renommage_espaces(spark):
    df = creer_df(spark, [" order_id ", " channel "])
    resultat, ecarts = aligner_schema(df, ["order_id", "channel"])

    assert resultat.first()["order_id"] == "valeur_0"
    assert len(ecarts["colonnes_renommees"]) == 2


def test_07_alias_explicite(spark):
    df = creer_df(spark, ["order_id", "canal"])
    resultat, ecarts = aligner_schema(
        df,
        ["order_id", "channel"],
        alias={"canal": "channel"},
    )

    assert resultat.first()["channel"] == "valeur_1"
    assert ecarts["colonnes_renommees"] == ["canal->channel"]


def test_08_plusieurs_renommages(spark):
    df = creer_df(spark, ["ORDER_ID", "canal", "STATUS"])
    _, ecarts = aligner_schema(
        df,
        ["order_id", "channel", "status"],
        alias={"canal": "channel"},
    )

    assert len(ecarts["colonnes_renommees"]) == 3


def test_09_plusieurs_colonnes_ajoutees(spark):
    df = creer_df(
        spark,
        ["order_id", "discount_pct", "promotion_code"],
    )
    _, ecarts = aligner_schema(df, ["order_id"])

    assert ecarts["colonnes_ajoutees"] == [
        "discount_pct",
        "promotion_code",
    ]


def test_10_plusieurs_colonnes_retirees(spark):
    df = creer_df(spark, ["order_id"])
    resultat, ecarts = aligner_schema(
        df,
        ["order_id", "channel", "status"],
    )

    assert ecarts["colonnes_retirees"] == ["channel", "status"]
    assert resultat.first()["channel"] is None
    assert resultat.first()["status"] is None


def test_11_order_id_obligatoire(spark):
    df = creer_df(spark, ["channel", "discount_pct"])

    with pytest.raises(ValueError, match="order_id"):
        aligner_schema(df, ["order_id", "channel"])


def test_12_colonnes_ambigues(spark):
    df = creer_df(spark, ["order_id", "CHANNEL", " Channel "])

    with pytest.raises(ValueError, match="ambigu"):
        aligner_schema(df, ["order_id", "channel"])


def test_13_colonnes_techniques_conservees(spark):
    techniques = [
        "livraison",
        "ingest_date",
        "_source_file",
        "_ingest_ts",
        "_corrupt_record",
    ]
    df = creer_df(spark, ["order_id", "channel"] + techniques)

    resultat, ecarts = aligner_schema(df, ["order_id", "channel"])

    assert resultat.columns == ["order_id", "channel"] + techniques
    assert ecarts["colonnes_ajoutees"] == []
    assert resultat.first()["_corrupt_record"] == "valeur_6"


def test_14_ordre_des_colonnes(spark):
    df = creer_df(spark, ["status", "channel", "order_id"])
    resultat, _ = aligner_schema(
        df,
        ["order_id", "channel", "status"],
    )

    assert resultat.columns == ["order_id", "channel", "status"]
    assert resultat.first()["order_id"] == "valeur_2"


def test_15_livraison_inconnue(spark):
    with pytest.raises(ValueError, match="Livraison inconnue"):
        lire_livraison(spark, "/data/bronze/commandes", "J9")


def test_16_partitions_j1_j2_independantes(spark, tmp_path):
    """J2 ajoute une colonne, mais elle ne doit pas apparaître en J1."""

    chemin = str(tmp_path / "bronze_commandes")

    j1 = spark.createDataFrame(
        [("C001", "WEB", "J1", "2026-02-01")],
        ["order_id", "channel", "livraison", "ingest_date"],
    )

    j2 = spark.createDataFrame(
        [("C002", "10", "J2", "2026-03-01")],
        ["order_id", "discount_pct", "livraison", "ingest_date"],
    )

    j1.write.mode("append").partitionBy("ingest_date").parquet(chemin)
    j2.write.mode("append").partitionBy("ingest_date").parquet(chemin)

    lu_j1 = lire_livraison(spark, chemin, "J1")
    lu_j2 = lire_livraison(spark, chemin, "J2")

    assert "channel" in lu_j1.columns
    assert "discount_pct" not in lu_j1.columns

    assert "discount_pct" in lu_j2.columns
    assert "channel" not in lu_j2.columns

    assert lu_j1.count() == 1
    assert lu_j2.count() == 1
