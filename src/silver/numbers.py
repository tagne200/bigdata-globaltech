"""Parsing des nombres (ticket BG-29).

Piège : cast("double") sur "205,16" ne plante pas, il renvoie NULL en silence.
On perdrait donc le prix sans aucune alerte. On remplace d'abord la virgule
décimale par un point, puis on convertit.
"""
from pyspark.sql import Column
from pyspark.sql import functions as F

from src.silver.schemas import PRICE_TYPE


def parse_price(col: Column) -> Column:
    """'205,16' -> 205.16 ; '83.34' -> 83.34 ; '' ou 'abc' -> null.

    Un prix négatif est converti tel quel : c'est BG-30 qui le rejettera.
    """
    s = F.regexp_replace(F.trim(col), ",", ".")
    return F.when(s.rlike(r"^-?\d+(\.\d+)?$"), s.cast(PRICE_TYPE))
