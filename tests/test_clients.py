import hashlib
from datetime import date

import pytest
from pyspark.sql import functions as F

from src.silver.dates import parse_date_multi
from src.silver.nettoyage_clients import (COLONNES_SUPPRIMEES, evaluer_clients, separer_clients)
from src.silver.pseudonymisation import email_hache, lire_sel, table_du_sel

SEL = "sel-de-test-uniquement-pas-un-vrai-secret-123"
DDL = ("customer_id string, company_name string, contact_first_name string, "
       "contact_last_name string, email string, phone string, country string, city string, "
       "segment string, created_at string, updated_at string, _corrupt_record string, "
       "livraison string")
COLS = [c.split()[0] for c in DDL.split(", ")]
BASE = dict(customer_id="C1", company_name="Systèmes Janssen SAS", contact_first_name="Piotr",
            contact_last_name="Wang", email="piotr.wang27@janssen.fr", phone="+33 6 12 34 56 78",
            country="France", city="Lyon", segment="PME", created_at="2020-06-23",
            updated_at="2025-06-17T14:39:13", _corrupt_record=None, livraison="J1")


def _traiter(spark, *lignes, sel=SEL):
    rows = [tuple({**BASE, **l}[c] for c in COLS) for l in lignes]
    evalue = evaluer_clients(spark.createDataFrame(rows, DDL), table_du_sel(spark, sel))
    return evalue, separer_clients(evalue, "run1"), len(rows)


def _sha(sel, email):
    return hashlib.sha256((sel + email).encode("utf-8")).hexdigest()


def test_l_email_est_hache_avec_sel_sha256_et_en_minuscules(spark):
    _, (propres, _, _), _ = _traiter(spark, {"email": "  Piotr.WANG27@Janssen.FR "})
    assert propres.first()["email_hash"] == _sha(SEL, "piotr.wang27@janssen.fr")


def test_meme_email_avec_casse_differente_donne_le_meme_hachage(spark):
    _, (propres, _, _), _ = _traiter(spark, {"customer_id": "C1", "email": "A@B.FR"},
                                     {"customer_id": "C2", "email": "a@b.fr"})
    assert len({r["email_hash"] for r in propres.collect()}) == 1


def test_un_autre_sel_donne_un_autre_hachage(spark):
    _, (p1, _, _), _ = _traiter(spark, {}, sel=SEL)
    _, (p2, _, _), _ = _traiter(spark, {}, sel=SEL + "x")
    assert p1.first()["email_hash"] != p2.first()["email_hash"]


def test_aucune_donnee_personnelle_en_clair_en_silver(spark):
    _, (propres, _, _), _ = _traiter(spark, {})
    assert not set(COLONNES_SUPPRIMEES) & set(propres.columns)
    ligne = " ".join(str(v) for v in propres.first())
    for secret in ["Piotr", "Wang", "+33 6 12 34 56 78", "piotr.wang27@janssen.fr"]:
        assert secret not in ligne


def test_le_sel_n_apparait_pas_dans_le_plan_d_execution(spark):
    evalue, _, _ = _traiter(spark, {})
    plan = evalue._jdf.queryExecution().toString()
    assert SEL not in plan


@pytest.mark.parametrize("brut", [None, "", "   ", "pas-un-email", "eva.jones59_at_firme.fr"])
def test_email_absent_ou_mal_forme_garde_le_client_sans_hachage(spark, brut):
    _, (propres, rejets, _), _ = _traiter(spark, {"email": brut})
    assert rejets.count() == 0 and propres.count() == 1
    assert propres.first()["email_hash"] is None and propres.first()["segment"] == "PME"


def test_doublon_exact_apres_normalisation_espaces_et_casse(spark):
    # cas réel de J1 : seuls les espaces du nom de société et la casse de l'email diffèrent
    _, (propres, _, doublons), _ = _traiter(
        spark, {"company_name": "  Énergie Li SL", "email": "PIOTR@LI.ES"},
        {"company_name": "Énergie Li SL", "email": "piotr@li.es"})
    assert propres.count() == 1 and doublons.first()["_doublon"] == "EXACT"
    assert propres.first()["company_name"] == "Énergie Li SL"


def test_meme_client_on_garde_la_mise_a_jour_la_plus_recente(spark):
    _, (propres, _, doublons), _ = _traiter(
        spark, {"city": "Lyon", "updated_at": "2025-01-01T10:00:00"},
        {"city": "Paris", "updated_at": "2025-06-01T10:00:00"},
        {"city": "Nice", "updated_at": "2024-01-01T10:00:00"})
    assert [r["city"] for r in propres.collect()] == ["Paris"]
    assert [r["_doublon"] for r in doublons.collect()] == ["ANCIEN", "ANCIEN"]


def test_le_resultat_ne_depend_pas_de_l_ordre_des_lignes(spark):
    a = {"city": "Lyon", "updated_at": "2025-01-01T10:00:00"}
    b = {"city": "Paris", "updated_at": "2025-06-01T10:00:00"}
    _, (p1, _, _), _ = _traiter(spark, a, b)
    _, (p2, _, _), _ = _traiter(spark, b, a)
    assert p1.first()["city"] == p2.first()["city"] == "Paris"


@pytest.mark.parametrize("champ, valeur, motif", [
    ("customer_id", "", "CLIENT_ID_INVALIDE"),
    ("updated_at", "N/A", "DATE_INVALIDE"),
])
def test_rejets(spark, champ, valeur, motif):
    _, (propres, rejets, _), _ = _traiter(spark, {champ: valeur})
    assert propres.count() == 0 and rejets.first()["motifs"] == [motif]


def test_ligne_mal_formee_ne_garde_que_format_invalide(spark):
    _, (propres, rejets, _), _ = _traiter(spark, {"_corrupt_record": "C1,x", "updated_at": "N/A"})
    assert rejets.first()["motifs"] == ["FORMAT_INVALIDE"]


def test_les_rejets_ne_contiennent_aucune_donnee_personnelle(spark):
    _, (_, rejets, _), _ = _traiter(spark, {"updated_at": "N/A"})
    preuve = rejets.first()["ligne_origine"]
    for secret in ["Piotr", "Wang", "+33", "piotr.wang27@janssen.fr", "email", "phone"]:
        assert secret not in preuve
    assert '"customer_id":"C1"' in preuve   # la clé reste, pour retrouver la ligne en bronze


@pytest.mark.parametrize("brut, attendu", [("France", "FR"), (" fr ", "FR"), ("Nederland", "NL"),
                                           ("Italia", "IT"), ("", None), (None, None)])
def test_pays_normalise_ou_vide_sans_rejet(spark, brut, attendu):
    _, (propres, rejets, _), _ = _traiter(spark, {"country": brut})
    assert propres.first()["country"] == attendu and rejets.count() == 0


def test_dates_de_creation_dans_les_deux_formats(spark):
    df = spark.createDataFrame([("2020-06-23",), ("23/06/2020",), ("",), ("hier",)], "d string")
    out = [r["x"] for r in df.select(parse_date_multi(F.col("d")).alias("x")).collect()]
    assert out == [date(2020, 6, 23), date(2020, 6, 23), None, None]


def test_chaque_ligne_lue_est_comptee_une_seule_fois(spark):
    _, (propres, rejets, doublons), lues = _traiter(
        spark, {}, {}, {"customer_id": "C2"}, {"customer_id": "C3", "updated_at": "N/A"},
        {"customer_id": "C4", "_corrupt_record": "x"})
    assert propres.count() + rejets.count() + doublons.count() == lues == 5
    assert (propres.count(), rejets.count(), doublons.count()) == (2, 2, 1)


def test_lecture_du_sel_exige_une_valeur_assez_longue():
    assert lire_sel({"PSEUDO_SEL": SEL}) == SEL
    for env in [{}, {"PSEUDO_SEL": ""}, {"PSEUDO_SEL": "court"}]:
        with pytest.raises(ValueError, match="PSEUDO_SEL"):
            lire_sel(env)
