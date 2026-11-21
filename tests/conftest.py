import os
import sys

import pytest
from pyspark.sql import SparkSession

# Spark lance des "workers" Python pour exécuter le code. Sans précision, il appelle
# la commande `python3`, qui sous Windows pointe vers un raccourci du Microsoft Store
# (erreur "Python est introuvable" puis "Python worker failed to connect back").
# On impose donc le Python qui exécute les tests (sys.executable).
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


@pytest.fixture(scope="session")
def spark():
    """Une session Spark locale, partagée par tous les tests (démarrage coûteux).

    Le mode local est accepté ici : ce sont des tests unitaires, pas le traitement
    final qui, lui, tourne sur YARN.
    """
    session = (SparkSession.builder.master("local[1]").appName("tests")
               .config("spark.sql.session.timeZone", "Europe/Paris")
               .config("spark.sql.shuffle.partitions", "1")
               .config("spark.ui.enabled", "false")
               # Même chose côté configuration Spark : double sécurité.
               .config("spark.pyspark.python", sys.executable)
               .config("spark.pyspark.driver.python", sys.executable)
               .getOrCreate())
    yield session
    session.stop()