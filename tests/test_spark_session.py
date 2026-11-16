"""Vérifie que la session Spark porte les réglages imposés par le cadrage."""

import pytest

F = pytest.importorskip("pyspark.sql.functions")


def test_fuseau_horaire_paris(spark):
    assert spark.conf.get("spark.sql.session.timeZone") == "Europe/Paris"


def test_ecrasement_dynamique_des_partitions(spark):
    assert spark.conf.get("spark.sql.sources.partitionOverwriteMode") == "dynamic"


def test_chiffre_affaires_par_pays(spark):
    df = spark.createDataFrame(
        [("FR", 2, 10.0), ("FR", 1, 5.0), ("DE", 3, 1.0)], ["pays", "quantite", "prix"]
    )
    rows = df.groupBy("pays").agg(F.sum(F.col("quantite") * F.col("prix")).alias("ca")).collect()
    assert {r["pays"]: r["ca"] for r in rows} == {"FR": 25.0, "DE": 3.0}
