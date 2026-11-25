import io
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from src.mapreduce import mapper_ca, reducer_ca
from src.silver.pays import SYNONYMES as SYNONYMES_SILVER

RACINE = Path(__file__).resolve().parent.parent
ENTETE = "order_id,order_ts,customer_id,product_id,quantity,unit_price_eur,country,warehouse_id,channel,status"


def ligne(quantity="2", prix="10.50", pays="France", statut="COMPLETED", order_id="O1"):
    # un prix à virgule est entre guillemets dans le CSV, sinon la virgule couperait la ligne en deux champs
    prix = f'"{prix}"' if "," in prix else prix
    return f"{order_id},2026-01-13T14:16:49,C1,P1,{quantity},{prix},{pays},W1,web,{statut}"


# ---------- mapper : une ligne ----------
@pytest.mark.parametrize("texte, attendu", [
    (ligne(), ("ok", "FR", 2100)),                         # 2 x 10,50 € = 21,00 € = 2100 centimes
    (ligne(prix="10,50"), ("ok", "FR", 2100)),             # virgule décimale acceptée
    (ligne(prix="10.10", quantity="3"), ("ok", "FR", 3030)),
    (ligne(prix="0.125", quantity="1"), ("ok", "FR", 13)),  # arrondi au centime, moitié vers le haut
    (ligne(pays=" deutschland "), ("ok", "DE", 2100)),     # casse et espaces ignorés
    (ligne(pays="USA"), ("ok", "US", 2100)),
    (ligne(statut=" shipped "), ("ok", "FR", 2100)),
    (ligne(statut="CANCELLED"), ("exclues_annulees", None, None)),
    (ENTETE, ("en_tetes", None, None)),
    ("", ("invalides_format", None, None)),
    ("a,b,c", ("invalides_format", None, None)),
    ("O1,2026-01-13T14:16:49,C1,P1,2,10,50,France,W1,web,COMPLETED", ("invalides_format", None, None)),  # virgule non protégée
    (ligne(quantity="abc"), ("invalides_quantite", None, None)),
    (ligne(quantity="2 pcs"), ("invalides_quantite", None, None)),
    (ligne(quantity="0"), ("invalides_quantite", None, None)),
    (ligne(quantity="-1"), ("invalides_quantite", None, None)),
    (ligne(prix="-34.69"), ("invalides_prix", None, None)),
    (ligne(prix="0"), ("invalides_prix", None, None)),
    (ligne(prix=""), ("invalides_prix", None, None)),
    (ligne(pays=""), ("invalides_pays", None, None)),
    (ligne(pays="Atlantide"), ("invalides_pays", None, None)),
    (ligne(statut="PENDING"), ("invalides_statut", None, None)),
    (ligne(statut=""), ("invalides_statut", None, None)),
])
def test_traiter_ligne(texte, attendu):
    assert mapper_ca.traiter_ligne(texte) == attendu


def test_une_ligne_invalide_n_a_qu_une_seule_raison_la_premiere_dans_l_ordre():
    # quantité ET prix ET pays faux : seule la quantité est comptée
    assert mapper_ca.traiter_ligne(ligne(quantity="x", prix="x", pays="x"))[0] == "invalides_quantite"


# ---------- mapper : le flux complet et ses compteurs ----------
def _mapper(texte):
    sortie, erreur = io.StringIO(), io.StringIO()
    mapper_ca.main(io.StringIO(texte), sortie, erreur)
    compteurs = {}
    for l in erreur.getvalue().splitlines():
        assert l.startswith("reporter:counter:commandes,")
        _, nom, valeur = l[len("reporter:counter:"):].split(",")[0:3]
        compteurs[nom] = int(valeur)
    return sortie.getvalue(), compteurs


def test_les_compteurs_suivent_le_format_reporter_de_hadoop_et_sont_coherents():
    texte = "\n".join([ENTETE, ligne(), ligne(statut="CANCELLED"), ligne(quantity="abc"), "", ligne(pays="DE")]) + "\n"
    sortie, c = _mapper(texte)
    assert c == {"lignes_lues": 6, "en_tetes": 1, "lignes_valides": 2, "exclues_annulees": 1,
                 "invalides_quantite": 1, "invalides_format": 1}
    # chaque ligne lue est comptée une seule fois
    assert c["lignes_lues"] == c["en_tetes"] + c["lignes_valides"] + c["exclues_annulees"] \
        + sum(v for k, v in c.items() if k.startswith("invalides_"))
    assert sortie.splitlines() == ["FR\t2100\t1", "DE\t2100\t1"]


def test_fins_de_ligne_windows_acceptees():
    sortie, c = _mapper(ENTETE + "\r\n" + ligne() + "\r\n")
    assert c["lignes_valides"] == 1 and c["en_tetes"] == 1


# ---------- reducer et combiner ----------
def _reducer(texte, *args):
    sortie, erreur = io.StringIO(), io.StringIO()
    reducer_ca.main(list(args), io.StringIO(texte), sortie, erreur)
    return sortie.getvalue(), erreur.getvalue()


def test_reducer_additionne_par_cle_triee():
    sortie, _ = _reducer("DE\t100\t1\nDE\t250\t1\nFR\t5\t2\n")
    assert sortie == "DE\t350\t2\nFR\t5\t2\n"


def test_reducer_final_affiche_des_euros():
    sortie, erreur = _reducer("DE\t1234567\t3\n", "--euros")
    assert sortie == "DE\t12345.67\t3\n" and "pays_en_sortie,1" in erreur
    assert _reducer("FR\t5\t1\n", "--euros")[0] == "FR\t0.05\t1\n"


def test_entree_vide():
    assert _reducer("")[0] == ""


def test_le_combiner_ne_change_pas_le_resultat():
    lignes = ["FR\t100\t1", "DE\t50\t1", "FR\t300\t1", "DE\t70\t1", "FR\t5\t1"]
    direct = _reducer("\n".join(sorted(lignes)) + "\n", "--euros")[0]
    moitie1 = _reducer("\n".join(sorted(lignes[:3])) + "\n")[0]     # pré-agrégat de deux "mappers"
    moitie2 = _reducer("\n".join(sorted(lignes[3:])) + "\n")[0]
    combine = _reducer("".join(sorted((moitie1 + moitie2).splitlines(True))), "--euros")[0]
    assert direct == combine == "DE\t1.20\t2\nFR\t4.05\t3\n"


# ---------- bout en bout : les scripts lancés comme Hadoop les lance ----------
def _lancer(script, entree, *args):
    r = subprocess.run([sys.executable, str(RACINE / "src" / "mapreduce" / script), *args],
                       input=entree, capture_output=True, text=True, encoding="utf-8", check=True)
    return r.stdout, r.stderr


def test_pipeline_complet_comme_hadoop_streaming():
    csv = "\n".join([ENTETE, ligne(order_id="1"), ligne(order_id="2", pays="DE", quantity="1", prix="99,99"),
                     ligne(order_id="3", statut="CANCELLED"), ligne(order_id="4", prix="-1"),
                     ligne(order_id="5", pays="france", quantity="4", prix="2.50")]) + "\n"
    paires, erreurs_mapper = _lancer("mapper_ca.py", csv)
    trie = "".join(sorted(paires.splitlines(True)))                 # le "shuffle and sort" de Hadoop
    final, _ = _lancer("reducer_ca.py", trie, "--euros")
    assert final == "DE\t99.99\t1\nFR\t31.00\t2\n"                  # 21,00 + 4 x 2,50
    assert "reporter:counter:commandes,invalides_prix,1" in erreurs_mapper


def test_la_table_des_pays_du_mapper_est_la_meme_que_celle_de_la_silver():
    assert mapper_ca.SYNONYMES == SYNONYMES_SILVER


@pytest.mark.skipif(
        os.name == "nt" or shutil.which("bash") is None,
        reason="Test Bash ignoré sous Windows ou si Bash est absent"
)
def test_le_script_de_lancement_est_syntaxiquement_correct():
    subprocess.run(["bash", "-n", str(RACINE / "scripts" / "ca_par_pays_streaming.sh")], check=True)
