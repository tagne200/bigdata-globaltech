"""Contrôles qualité des commandes en un seul passage (ticket BG-30).

Principe : on ajoute à chaque ligne une colonne `motifs` (liste de textes). Une ligne
sans motif est propre, une ligne avec au moins un motif est rejetée UNE SEULE fois,
avec la liste de tous ses motifs. Toutes les règles sont calculées dans la même
transformation Spark : on ne relit pas la table une fois par règle.

Motifs :
    FORMAT_INVALIDE     ligne mal formée en bronze (_corrupt_record). Les autres colonnes
                        sont alors peu fiables : on ne contrôle rien d'autre.
    DATE_INVALIDE       date illisible (N/A, hier, vide...)
    DATE_HORS_PERIODE   date lisible mais hors du mois de la livraison
    PAYS_INVALIDE       pays vide ou inconnu
    QUANTITE_INVALIDE   quantité non numérique, vide ou <= 0
    PRIX_INVALIDE       prix non numérique, vide ou <= 0
    PRODUIT_ORPHELIN    product_id absent du référentiel produits
    CLIENT_ORPHELIN     customer_id absent du référentiel clients
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.silver.dates import parse_order_ts
from src.silver.nombres import parse_price
from src.silver.pays import normaliser_pays
from src.silver.schemas import SILVER_COMMANDES, imposer_schema

COLONNES_METIER = [
    "order_id", "order_ts", "customer_id", "product_id", "quantity",
    "unit_price_eur", "country", "warehouse_id", "channel", "status",
]


def _quantite(col):
    """'3' -> 3 ; 'abc', '2 pcs', '-1', '' -> null (le <= 0 est contrôlé après)."""
    s = F.trim(col)
    return F.when(s.rlike(r"^\d+$"), s.cast("int"))


def _referentiel(df: DataFrame, cle: str, nom_drapeau: str) -> DataFrame:
    """Liste des clés connues, avec un drapeau True : sert à repérer les orphelines."""
    return (df.select(F.col(cle).alias("_cle")).distinct()
            .withColumn(nom_drapeau, F.lit(True)))


def evaluer_commandes(commandes: DataFrame, produits: DataFrame, clients: DataFrame,
                      debut: str, fin: str) -> DataFrame:
    """Ajoute les valeurs converties (_ts, _qte, _prix, _pays) et la colonne `motifs`.

    debut / fin : bornes de la période autorisée, au format 'AAAA-MM-JJ' (fin exclue).
    """
    ts = parse_order_ts(F.col("order_ts"))
    qte = _quantite(F.col("quantity"))
    prix = parse_price(F.col("unit_price_eur"))
    pays = normaliser_pays(F.col("country"))
    borne_debut = F.lit(debut).cast("timestamp")
    borne_fin = F.lit(fin).cast("timestamp")

    prod = _referentiel(produits, "product_id", "_produit_connu")
    cli = _referentiel(clients, "customer_id", "_client_connu")
    # Les référentiels sont petits : broadcast = pas de mélange (shuffle) des commandes.
    df = (commandes
          .join(F.broadcast(prod), F.col("product_id") == prod["_cle"], "left").drop("_cle")
          .join(F.broadcast(cli), F.col("customer_id") == cli["_cle"], "left").drop("_cle"))

    regles = [
        ("DATE_INVALIDE", ts.isNull()),
        ("DATE_HORS_PERIODE", ts.isNotNull() & ((ts < borne_debut) | (ts >= borne_fin))),
        ("PAYS_INVALIDE", pays.isNull()),
        ("QUANTITE_INVALIDE", qte.isNull() | (qte <= 0)),
        ("PRIX_INVALIDE", prix.isNull() | (prix <= 0)),
        ("PRODUIT_ORPHELIN", F.col("_produit_connu").isNull()),
        ("CLIENT_ORPHELIN", F.col("_client_connu").isNull()),
    ]
    # when(cond, motif) vaut null si la règle est respectée : filter retire ces null.
    motifs_regles = F.filter(
        F.array(*[F.when(cond, F.lit(nom)) for nom, cond in regles]),
        lambda m: m.isNotNull(),
    )
    motifs = (F.when(F.col("_corrupt_record").isNotNull(), F.array(F.lit("FORMAT_INVALIDE")))
              .otherwise(motifs_regles))

    return (df.withColumn("_ts", ts).withColumn("_qte", qte)
            .withColumn("_prix", prix).withColumn("_pays", pays)
            .withColumn("motifs", motifs)
            .drop("_produit_connu", "_client_connu"))


def separer(evalue: DataFrame, run_id: str):
    """Sépare les lignes propres (typées) et les rejets (avec motifs et ligne d'origine)."""
    propres = imposer_schema(
        evalue.filter(F.size("motifs") == 0).select(
            "order_id", F.col("_ts").alias("order_ts"), "customer_id", "product_id",
            F.col("_qte").alias("quantity"), F.col("_prix").alias("unit_price_eur"),
            F.col("_pays").alias("country"), "warehouse_id", "channel",
            # statut normalisé (trim + majuscules) : le CA de la gold exclut CANCELLED
            # et BG-31 classe les statuts, donc "completed" doit devenir "COMPLETED"
            F.upper(F.trim("status")).alias("status"),
            "livraison", "ingest_date"),
        SILVER_COMMANDES, colonnes_en_plus=["livraison", "ingest_date"])

    rejets = evalue.filter(F.size("motifs") > 0).select(
        F.lit("commandes").alias("source"), "livraison", "ingest_date", "motifs",
        # la ligne telle qu'elle est arrivée en bronze : c'est la preuve du rejet
        F.to_json(F.struct(*COLONNES_METIER, "_corrupt_record")).alias("ligne_origine"),
        F.lit(run_id).alias("run_id"), F.current_timestamp().alias("rejete_le"))
    return propres, rejets