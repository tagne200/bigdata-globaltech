#!/usr/bin/env python3
"""Reducer (et combiner) Hadoop Streaming : somme du chiffre d'affaires par pays (ticket BG-46).

Entre le mapper et le reducer, Hadoop TRIE les paires par clé (c'est le "shuffle and sort") :
toutes les lignes d'un même pays arrivent donc à la suite, et il suffit de les additionner
jusqu'au changement de clé.

Entrée et sortie ont la même forme : PAYS <TAB> centimes <TAB> nombre_de_commandes.
C'est ce qui permet de réutiliser ce script comme COMBINER : un pré-agrégat fait sur la machine
du mapper, avant l'envoi sur le réseau, qui réduit le volume échangé sans changer le résultat.
Avec --euros (réducteur final), le montant est affiché en euros avec deux décimales.
"""
import io
import sys

GROUPE = "commandes"


def en_euros(centimes):
    return f"{centimes // 100}.{centimes % 100:02d}"


def main(argv=None, entree=None, sortie=None, erreur=None):
    argv = sys.argv[1:] if argv is None else argv
    euros = "--euros" in argv
    entree = entree or io.TextIOWrapper(sys.stdin.buffer, encoding="utf-8", errors="replace")
    sortie = sortie or sys.stdout
    erreur = erreur or sys.stderr

    cle, centimes, nombre, groupes = None, 0, 0, 0

    def ecrire():
        sortie.write(f"{cle}\t{en_euros(centimes) if euros else centimes}\t{nombre}\n")

    for ligne in entree:
        ligne = ligne.rstrip("\r\n")
        if not ligne:
            continue
        pays, c, n = ligne.split("\t")
        if pays != cle:
            if cle is not None:
                ecrire()
                groupes += 1
            cle, centimes, nombre = pays, 0, 0
        centimes += int(c)
        nombre += int(n)
    if cle is not None:
        ecrire()
        groupes += 1
    if euros:   # le compteur n'est utile qu'une fois, au réducteur final
        erreur.write(f"reporter:counter:{GROUPE},pays_en_sortie,{groupes}\n")


if __name__ == "__main__":
    main()
