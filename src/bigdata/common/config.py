"""Chargement de la configuration externalisée.

Ordre de priorité (du plus faible au plus fort) :
1. le fichier JSON désigné par APP_CONF (par défaut conf/app.json) ;
2. les variables d'environnement préfixées par BIGDATA_ (ex. BIGDATA_DELIVERY=J2).

Les secrets (sel de pseudonymisation...) ne sont jamais dans le JSON :
ils sont lus uniquement depuis l'environnement.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

DEFAULT_CONF = Path(__file__).resolve().parents[3] / "conf" / "app.json"
ENV_PREFIX = "BIGDATA_"
REQUIRED_KEYS = ("app_name", "input_dir", "delivery", "output_dir", "shuffle_partitions")
VALID_DELIVERIES = ("J1", "J2", "J3")


class ConfigError(ValueError):
    """Configuration absente, incomplète ou invalide."""


def _cast(value: str, current: Any) -> Any:
    """Convertit une valeur d'environnement dans le type de la valeur du JSON."""
    if isinstance(current, bool):
        return value.strip().lower() in ("1", "true", "yes", "oui")
    if isinstance(current, int):
        try:
            return int(value)
        except ValueError as exc:
            raise ConfigError(f"Entier attendu, reçu {value!r}") from exc
    return value


def load_config(path: str | os.PathLike | None = None, env: dict | None = None) -> dict:
    """Charge la configuration, applique les surcharges d'environnement et la valide."""
    env = dict(os.environ) if env is None else env
    conf_path = Path(path or env.get("APP_CONF") or DEFAULT_CONF)
    if not conf_path.is_file():
        raise ConfigError(f"Fichier de configuration introuvable : {conf_path}")
    try:
        conf = json.loads(conf_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ConfigError(f"JSON invalide dans {conf_path} : {exc}") from exc

    for key, value in env.items():
        if key.startswith(ENV_PREFIX):
            name = key[len(ENV_PREFIX) :].lower()
            conf[name] = _cast(value, conf.get(name))

    missing = [k for k in REQUIRED_KEYS if k not in conf]
    if missing:
        raise ConfigError(f"Clés manquantes dans la configuration : {', '.join(missing)}")
    if conf["delivery"] not in VALID_DELIVERIES:
        raise ConfigError(f"Livraison inconnue : {conf['delivery']!r} (attendu J1, J2 ou J3)")
    if int(conf["shuffle_partitions"]) <= 0:
        raise ConfigError("shuffle_partitions doit être strictement positif")
    return conf
