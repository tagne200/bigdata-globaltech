import json

import pytest

from bigdata.common.config import ConfigError, load_config

BASE = {
    "app_name": "test",
    "input_dir": "/data",
    "delivery": "J1",
    "output_dir": "/out",
    "shuffle_partitions": 8,
}


@pytest.fixture
def conf_file(tmp_path):
    def _write(content):
        path = tmp_path / "app.json"
        path.write_text(content if isinstance(content, str) else json.dumps(content), encoding="utf-8")
        return path

    return _write


def test_charge_le_fichier(conf_file):
    conf = load_config(conf_file(BASE), env={})
    assert conf["delivery"] == "J1"
    assert conf["shuffle_partitions"] == 8


def test_variable_environnement_surcharge_et_garde_le_type(conf_file):
    env = {"BIGDATA_DELIVERY": "J2", "BIGDATA_SHUFFLE_PARTITIONS": "16"}
    conf = load_config(conf_file(BASE), env=env)
    assert conf["delivery"] == "J2"
    assert conf["shuffle_partitions"] == 16


def test_fichier_absent(tmp_path):
    with pytest.raises(ConfigError, match="introuvable"):
        load_config(tmp_path / "absent.json", env={})


def test_json_invalide(conf_file):
    with pytest.raises(ConfigError, match="JSON invalide"):
        load_config(conf_file("{pas du json"), env={})


def test_cle_manquante(conf_file):
    incomplet = {k: v for k, v in BASE.items() if k != "output_dir"}
    with pytest.raises(ConfigError, match="output_dir"):
        load_config(conf_file(incomplet), env={})


@pytest.mark.parametrize("livraison", ["J4", "j1", ""])
def test_livraison_inconnue(conf_file, livraison):
    with pytest.raises(ConfigError, match="Livraison inconnue"):
        load_config(conf_file({**BASE, "delivery": livraison}), env={})


def test_partitions_non_numeriques(conf_file):
    with pytest.raises(ConfigError, match="Entier attendu"):
        load_config(conf_file(BASE), env={"BIGDATA_SHUFFLE_PARTITIONS": "beaucoup"})


def test_partitions_nulles(conf_file):
    with pytest.raises(ConfigError, match="strictement positif"):
        load_config(conf_file({**BASE, "shuffle_partitions": 0}), env={})


def test_configuration_du_depot_est_valide():
    """Le fichier conf/app.json versionné doit toujours se charger."""
    conf = load_config(env={})
    assert conf["delivery"] in ("J1", "J2", "J3")
