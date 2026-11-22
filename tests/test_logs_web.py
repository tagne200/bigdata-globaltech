
"""Tests du parsing des logs web — BG-34."""

from src.silver.parsing_logs import parser_logs


def test_log_valide(spark):
    """Un log Apache correct doit être analysé sans rejet."""

    ligne = (
        '192.0.2.218 - - [01/Jan/2026:00:03:49 +0100] '
        '"GET / HTTP/1.1" 200 16777 '
        '"https://www.google.com/" '
        '"Googlebot/2.1 (+http://www.google.com/bot.html)" 51'
    )

    df = spark.createDataFrame([(ligne,)], ["ligne"])
    resultat = parser_logs(df).first()

    assert resultat["ip"] == "192.0.2.218"
    assert resultat["methode"] == "GET"
    assert resultat["url"] == "/"
    assert resultat["statut_http"] == 200
    assert resultat["taille_octets"] == 16777
    assert resultat["temps_reponse"] == 51
    assert resultat["horodatage"] is not None
    assert resultat["motif_rejet"] is None


def test_log_tronque(spark):
    """Une ligne incomplète doit être rejetée."""

    ligne = '192.0.2.218 - - [01/Jan/2026:00:03:49 +0100] "GET /'

    df = spark.createDataFrame([(ligne,)], ["ligne"])
    resultat = parser_logs(df).first()

    assert resultat["motif_rejet"] == "FORMAT_INVALIDE"


def test_statut_http_invalide(spark):
    """Un code HTTP hors plage doit être rejeté."""

    ligne = (
        '192.0.2.218 - - [01/Jan/2026:00:03:49 +0100] '
        '"GET / HTTP/1.1" 700 100 "-" "Mozilla/5.0" 51'
    )

    df = spark.createDataFrame([(ligne,)], ["ligne"])
    resultat = parser_logs(df).first()

    assert resultat["motif_rejet"] == "STATUT_HTTP_INVALIDE"


def test_comptage_logs(spark):
    """Aucune ligne ne doit disparaître pendant le parsing."""

    valide = (
        '192.0.2.47 - - [01/Jan/2026:00:03:11 +0100] '
        '"GET /static/main.css HTTP/1.1" 200 17757 '
        '"-" "Mozilla/5.0" 57'
    )
    tronque = '192.0.2.47 - - [01/Jan/2026:00:03:11'

    df = spark.createDataFrame(
        [(valide,), (tronque,)],
        ["ligne"]
    )

    resultat = parser_logs(df)

    assert resultat.count() == 2
    assert resultat.filter("motif_rejet IS NULL").count() == 1
    assert resultat.filter("motif_rejet IS NOT NULL").count() == 1
