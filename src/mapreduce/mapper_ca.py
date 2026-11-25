#!/usr/bin/env python3
"""Mapper Hadoop Streaming : chiffre d'affaires par pays (ticket BG-46).

Hadoop Streaming lance ce script comme un processus : il lit les lignes du fichier CSV sur
l'entrée standard (stdin) et écrit des paires "clé<TAB>valeur" sur la sortie standard (stdout).
Ce que le script écrit sur la sortie d'erreur (stderr) au format "reporter:counter:..." est lu
par Hadoop comme un COMPTEUR : il est additionné sur toutes les machines et affiché à la fin du job.

Pour chaque ligne de commande valide, on émet :   PAYS <TAB> chiffre_d_affaires_en_centimes <TAB> 1

Règles (identiques en Spark, BG-47, pour que la comparaison soit honnête) :
  - la ligne d'en-tête est ignorée (compteur en_tetes) ;
  - une ligne est INVALIDE pour une seule raison, la première rencontrée dans cet ordre :
    format (pas 10 champs), quantite (entier > 0), prix (nombre > 0, virgule acceptée),
    pays (inconnu ou vide), statut (hors SHIPPED, COMPLETED, RETURNED, CANCELLED) ;
  - une commande CANCELLED n'entre pas dans le chiffre d'affaires (compteur exclues_annulees) ;
  - pas de dédoublonnage et pas de contrôle de période : c'est un calcul brut, volontairement.
Le montant est calculé en CENTIMES entiers : une somme d'entiers est exacte, pas celle de flottants.
"""
import csv
import io
import re
import sys
from decimal import ROUND_HALF_UP, Decimal

GROUPE = "commandes"
NB_CHAMPS = 10
# order_id, order_ts, customer_id, product_id, quantity, unit_price_eur, country, warehouse_id, channel, status
I_QUANTITE, I_PRIX, I_PAYS, I_STATUT = 4, 5, 6, 9

# Même table que src/silver/pays.py (un test vérifie qu'elles restent identiques).
# Elle est recopiée ici : le mapper tourne seul sur les machines du cluster, sans Spark.
SYNONYMES = {
    "FR": ["FR", "FRA", "FRANCE"],
    "DE": ["DE", "DEU", "DEUTSCHLAND"],
    "ES": ["ES", "ESP", "ESPAÑA"],
    "IT": ["IT", "ITA", "ITALIA"],
    "GB": ["GB", "GBR", "UNITED KINGDOM"],
    "US": ["US", "USA"],
    "PL": ["PL", "POL", "POLSKA"],
    "CN": ["CN", "CHN", "CHINA"],
    "BE": ["BE", "BEL", "BELGIQUE"],
    "NL": ["NL", "NLD", "NEDERLAND"],
}
CODE_PAYS = {ecriture: iso for iso, liste in SYNONYMES.items() for ecriture in liste}
STATUTS = {"SHIPPED", "COMPLETED", "RETURNED", "CANCELLED"}
FORME_PRIX = re.compile(r"^\d+(\.\d+)?$")


def traiter_ligne(ligne):
    """Renvoie ('ok', pays, centimes) ou (nom_du_compteur, None, None)."""
    if ligne.strip().lower().startswith("order_id,"):
        return "en_tetes", None, None
    try:
        champs = next(csv.reader([ligne]))
    except (csv.Error, StopIteration):
        return "invalides_format", None, None
    if len(champs) != NB_CHAMPS:
        return "invalides_format", None, None

    quantite = champs[I_QUANTITE].strip()
    if not quantite.isdigit() or int(quantite) <= 0:
        return "invalides_quantite", None, None
    prix = champs[I_PRIX].strip().replace(",", ".")
    if not FORME_PRIX.match(prix) or Decimal(prix) <= 0:
        return "invalides_prix", None, None
    pays = CODE_PAYS.get(champs[I_PAYS].strip().upper())
    if pays is None:
        return "invalides_pays", None, None
    statut = champs[I_STATUT].strip().upper()
    if statut not in STATUTS:
        return "invalides_statut", None, None
    if statut == "CANCELLED":
        return "exclues_annulees", None, None

    centimes_unitaire = int((Decimal(prix) * 100).quantize(Decimal(1), rounding=ROUND_HALF_UP))
    return "ok", pays, centimes_unitaire * int(quantite)


def main(entree=None, sortie=None, erreur=None):
    entree = entree or io.TextIOWrapper(sys.stdin.buffer, encoding="utf-8", errors="replace")
    sortie = sortie or sys.stdout
    erreur = erreur or sys.stderr
    compteurs = {}
    for ligne in entree:
        ligne = ligne.rstrip("\r\n")
        compteurs["lignes_lues"] = compteurs.get("lignes_lues", 0) + 1
        resultat, pays, centimes = traiter_ligne(ligne)
        if resultat == "ok":
            compteurs["lignes_valides"] = compteurs.get("lignes_valides", 0) + 1
            sortie.write(f"{pays}\t{centimes}\t1\n")
        else:
            compteurs[resultat] = compteurs.get(resultat, 0) + 1
    # Un compteur par type, envoyé une fois à la fin : "reporter:counter:groupe,nom,valeur"
    for nom, valeur in sorted(compteurs.items()):
        erreur.write(f"reporter:counter:{GROUPE},{nom},{valeur}\n")


if __name__ == "__main__":
    main()
