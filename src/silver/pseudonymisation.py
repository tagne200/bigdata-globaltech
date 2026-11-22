"""Pseudonymisation des clients (ticket BG-33).

L'email sert de clé de rapprochement, mais il identifie une personne : on ne le garde jamais
en clair en silver. On le remplace par un hachage SALÉ :

    email_hash = sha2( sel || email_en_minuscules , 256 )

- haché  : on ne peut pas retrouver l'email à partir de la valeur stockée ;
- salé   : sans le sel, impossible de retrouver un email en hachant une liste d'emails connus
           (attaque par dictionnaire) ;
- stable : le même email donne toujours le même hachage, donc on peut compter et joindre les
           clients sans connaître leur identité.
Le sel n'est JAMAIS dans Git : il vient de la variable d'environnement PSEUDO_SEL.
"""
import os

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

VARIABLE_SEL = "PSEUDO_SEL"
LONGUEUR_MINI = 16  # un sel trop court se devine plus facilement


def lire_sel(environ=None) -> str:
    """Lit le sel dans l'environnement. Pas de valeur par défaut : un sel écrit dans le code
    finirait dans Git, ce que le projet interdit."""
    sel = (environ if environ is not None else os.environ).get(VARIABLE_SEL, "")
    if len(sel) < LONGUEUR_MINI:
        raise ValueError(
            f"Variable d'environnement {VARIABLE_SEL} absente ou trop courte "
            f"(au moins {LONGUEUR_MINI} caractères). Voir docs/adr/004-pseudonymisation-clients.md.")
    return sel


def table_du_sel(spark: SparkSession, sel: str) -> DataFrame:
    """Le sel dans un DataFrame d'une ligne, joint aux données.

    On évite F.lit(sel) : un littéral apparaît en clair dans le plan d'exécution, donc dans
    les journaux de Spark et l'historique (History Server). Dans un DataFrame, il n'est pas
    imprimé dans le plan."""
    return spark.createDataFrame([(sel,)], "_sel string")


def email_hache(sel: Column, email: Column) -> Column:
    """sha2(sel || email en minuscules, 256) : 64 caractères hexadécimaux."""
    return F.sha2(F.concat(sel, F.lower(F.trim(email))), 256)
