"""Journalisation commune : chaque ligne porte le job, la livraison et le run_id."""

from __future__ import annotations

import logging
import sys
import uuid

FORMAT = "%(asctime)s %(levelname)s [%(job)s|%(delivery)s|%(run_id)s] %(name)s - %(message)s"


def new_run_id() -> str:
    """Identifiant court et unique d'une exécution, repris dans la table d'audit."""
    return uuid.uuid4().hex[:12]


def get_logger(job: str, delivery: str, run_id: str | None = None) -> logging.LoggerAdapter:
    """Retourne un logger qui ajoute le contexte d'exécution à chaque message."""
    logger = logging.getLogger(f"bigdata.{job}")
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter(FORMAT))
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False
    context = {"job": job, "delivery": delivery, "run_id": run_id or new_run_id()}
    return logging.LoggerAdapter(logger, context)
