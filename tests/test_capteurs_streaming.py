"""Tests de la lecture du flux capteurs (BG-42)."""
import json
import os

import pytest
from pyspark.sql import functions as F

from src.streaming.capteurs import (SCHEMA_CAPTEURS, bilan_du_lot, lire_flux,
                                    preparer_evenements)


def evenement(**champs):
    base = {"event_id": "EVT-1", "sensor_id": "S-FR-LYO-02-M08-TEMP", "machine_id": "FR-LYO-02-M08",
            "site": "FR-LYO-02", "country": "FR", "metric": "temperature", "value": 58.14,
            "unit": "C", "event_time": "2026-01-31T08:00:02.110+01:00"}
    base.update(champs)
    return json.dumps(base)


def preparer(spark, tmp_path, lignes):
    fichier = tmp_path / "capteurs_test.jsonl"
    fichier.write_text("\n".join(lignes) + "\n", encoding="utf-8")
    brut = (spark.read.schema(SCHEMA_CAPTEURS).option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", "_corrupt_record").json(str(fichier)))
    # Heure formatée par Spark (fuseau de session Europe/Paris) : un datetime récupéré par
    # collect() dépendrait du fuseau de la machine qui lance les tests (UTC en CI).
    return (preparer_evenements(brut)
            .withColumn("heure", F.date_format("horodatage", "yyyy-MM-dd HH:mm:ss.SSS"))
            .collect())


def test_evenement_valide(spark, tmp_path):
    (ligne,) = preparer(spark, tmp_path, [evenement()])
    assert ligne["motifs"] == []
    assert ligne["valeur"] == pytest.approx(58.14)
    # +01:00 dans la source = 08:00:02 à Paris (fuseau de la session de test)
    assert ligne["heure"] == "2026-01-31 08:00:02.110"


def test_valeur_envoyee_en_texte_est_gardee(spark, tmp_path):
    """22 lignes de J1 ont "value":"58.14" : elles doivent rester valides."""
    (ligne,) = preparer(spark, tmp_path, [evenement(value="58.14")])
    assert ligne["motifs"] == []
    assert ligne["valeur"] == pytest.approx(58.14)


def test_json_invalide_ne_garde_que_ce_motif(spark, tmp_path):
    (ligne,) = preparer(spark, tmp_path, ['{"event_id":"EVT-2","metric":"vibration",'])
    assert ligne["motifs"] == ["JSON_INVALIDE"]


@pytest.mark.parametrize("champs, motif", [
    ({"value": None}, "VALEUR_MANQUANTE"),
    ({"value": ""}, "VALEUR_MANQUANTE"),
    ({"value": "abc"}, "VALEUR_NON_NUMERIQUE"),
    ({"metric": "pressure"}, "METRIQUE_INCONNUE"),
    ({"metric": None}, "METRIQUE_INCONNUE"),
    ({"event_time": "hier"}, "DATE_INVALIDE"),
    ({"event_time": None}, "DATE_INVALIDE"),
    ({"event_id": " "}, "EVENT_ID_MANQUANT"),
])
def test_motif_de_rejet(spark, tmp_path, champs, motif):
    (ligne,) = preparer(spark, tmp_path, [evenement(**champs)])
    assert ligne["motifs"] == [motif]


def test_metrique_normalisee(spark, tmp_path):
    (ligne,) = preparer(spark, tmp_path, [evenement(metric=" Pression ")])
    assert ligne["motifs"] == []
    assert ligne["metrique"] == "pression"


def test_plusieurs_motifs_dans_une_seule_ligne(spark, tmp_path):
    (ligne,) = preparer(spark, tmp_path, [evenement(value=None, event_time="hier")])
    assert sorted(ligne["motifs"]) == ["DATE_INVALIDE", "VALEUR_MANQUANTE"]


def test_bilan_du_lot(spark, tmp_path):
    fichier = tmp_path / "lot.jsonl"
    fichier.write_text("\n".join([evenement(), evenement(value=None), "{pas du json"]) + "\n",
                       encoding="utf-8")
    brut = (spark.read.schema(SCHEMA_CAPTEURS).option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", "_corrupt_record").json(str(fichier)))
    bilan = bilan_du_lot(preparer_evenements(brut), 7)
    assert bilan == {"lot": 7, "lus": 3, "valides": 1, "invalides": 2,
                     "JSON_INVALIDE": 1, "VALEUR_MANQUANTE": 1}


def test_le_flux_ignore_les_fichiers_en_cours_de_copie(spark, tmp_path):
    """`hdfs dfs -put` écrit d'abord <nom>._COPYING_ : Spark ne doit jamais le lire."""
    landing = tmp_path / "landing"
    landing.mkdir()
    (landing / "capteurs_1.jsonl").write_text(evenement(event_id="A") + "\n", encoding="utf-8")
    (landing / "capteurs_2.jsonl._COPYING_").write_text(evenement(event_id="B") + "\n",
                                                        encoding="utf-8")
    vus = []

    def collecter(lot, _id):
        vus.extend(r["event_id"] for r in lot.select("event_id").collect())

    requete = (preparer_evenements(lire_flux(spark, str(landing))).writeStream
               .foreachBatch(collecter)
               .option("checkpointLocation", str(tmp_path / "checkpoint"))
               .trigger(availableNow=True).start())
    requete.awaitTermination()
    assert vus == ["A"]


# ---------------------------------------------------------------- BG-43 : déduplication et fenêtres

from src.streaming.capteurs import (agreger_par_fenetre, dedoublonner,  # noqa: E402
                                    evenements_valides, lignes_ecartees_par_le_watermark)


def lire_batch(spark, tmp_path, lignes, nom="lot.jsonl"):
    fichier = tmp_path / nom
    fichier.write_text("\n".join(lignes) + "\n", encoding="utf-8")
    return preparer_evenements(spark.read.schema(SCHEMA_CAPTEURS).option("mode", "PERMISSIVE")
                               .option("columnNameOfCorruptRecord", "_corrupt_record")
                               .json(str(fichier)))


def test_un_event_id_renvoye_n_est_compte_qu_une_fois(spark, tmp_path):
    df = lire_batch(spark, tmp_path, [evenement(event_id="A"), evenement(event_id="A"),
                                      evenement(event_id="B")])
    assert dedoublonner(evenements_valides(df)).count() == 2


def test_les_evenements_invalides_n_entrent_pas_dans_les_fenetres(spark, tmp_path):
    df = lire_batch(spark, tmp_path, [evenement(event_id="A"), evenement(event_id="B", value=None)])
    assert evenements_valides(df).count() == 1


def test_agregats_d_une_fenetre_d_une_minute(spark, tmp_path):
    df = lire_batch(spark, tmp_path, [
        evenement(event_id="A", value=10, event_time="2026-01-31T08:00:10+01:00"),
        evenement(event_id="B", value=20, event_time="2026-01-31T08:00:50+01:00"),
        evenement(event_id="C", value=99, event_time="2026-01-31T08:01:05+01:00"),  # minute suivante
    ])
    fenetres = {r["debut"]: r for r in agreger_par_fenetre(dedoublonner(evenements_valides(df)))
                .withColumn("debut", F.date_format("debut_fenetre", "HH:mm"))
                .withColumn("fin", F.date_format("fin_fenetre", "HH:mm")).collect()}
    assert sorted(fenetres) == ["08:00", "08:01"]
    f = fenetres["08:00"]
    assert (f["nb_mesures"], f["valeur_moyenne"], f["valeur_min"], f["valeur_max"]) == (2, 15.0, 10, 20)
    assert f["fin"] == "08:01"


def test_flux_watermark_deduplication_et_retard(spark, tmp_path):
    """Scénario en streaming, un fichier par micro-lot :
    lot 0 : A (08:00:10), A renvoyé, B (08:00:40)   -> une fenêtre 08:00 en attente
    lot 1 : C (08:10:00)                             -> watermark = 08:10 - 6 min = 08:04
    lot 2 : E (08:11:00)                             -> la fenêtre 08:00 devient définitive
    lot 3 : D (08:00:30), arrivé beaucoup trop tard  -> écarté par le watermark
    Attendu : la fenêtre 08:00 est écrite UNE fois, avec 2 mesures (A et B, A dédoublonné),
    et D est compté comme écarté. Les fenêtres 08:10 et 08:11 ne sont pas encore définitives.

    Subtilité mesurée (Spark 3.5, deux opérateurs à état enchaînés) : le filtre des événements
    en retard utilise le watermark du lot PRÉCÉDENT. Un événement tardif qui arrive dans le
    lot qui suit immédiatement la montée du watermark est donc encore accepté : la tolérance
    réelle est un peu plus large que 6 min, jamais plus courte. D'où le lot 2 intermédiaire.
    """
    landing = tmp_path / "landing"
    landing.mkdir()
    (landing / "capteurs_1.jsonl").write_text("\n".join([
        evenement(event_id="A", value=10, event_time="2026-01-31T08:00:10+01:00"),
        evenement(event_id="A", value=10, event_time="2026-01-31T08:00:10+01:00"),
        evenement(event_id="B", value=20, event_time="2026-01-31T08:00:40+01:00"),
    ]) + "\n", encoding="utf-8")
    (landing / "capteurs_2.jsonl").write_text(
        evenement(event_id="C", value=5, event_time="2026-01-31T08:10:00+01:00") + "\n",
        encoding="utf-8")
    (landing / "capteurs_3.jsonl").write_text(
        evenement(event_id="E", value=7, event_time="2026-01-31T08:11:00+01:00") + "\n",
        encoding="utf-8")
    (landing / "capteurs_4.jsonl").write_text(
        evenement(event_id="D", value=30, event_time="2026-01-31T08:00:30+01:00") + "\n",
        encoding="utf-8")

    # La source fichier traite les fichiers par date de modification : on l'impose,
    # sinon trois fichiers écrits dans la même milliseconde peuvent être lus dans le désordre.
    for i, nom in enumerate(["capteurs_1.jsonl", "capteurs_2.jsonl", "capteurs_3.jsonl",
                           "capteurs_4.jsonl"]):
        os.utime(landing / nom, (1_000_000 + i * 60, 1_000_000 + i * 60))

    ecrites = []

    def collecter(lot, _id):
        ecrites.extend(lot.withColumn("debut", F.date_format("debut_fenetre", "HH:mm")).collect())

    flux = agreger_par_fenetre(dedoublonner(evenements_valides(
        preparer_evenements(lire_flux(spark, str(landing), max_fichiers=1)))))
    requete = (flux.writeStream.outputMode("append").foreachBatch(collecter)
               .option("checkpointLocation", str(tmp_path / "checkpoint"))
               .trigger(availableNow=True).start())
    requete.awaitTermination()

    assert [(r["debut"], r["nb_mesures"], r["valeur_moyenne"]) for r in ecrites] == [("08:00", 2, 15.0)]
    assert lignes_ecartees_par_le_watermark(requete) == 1
