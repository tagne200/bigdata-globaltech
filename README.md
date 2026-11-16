# bigdata-globaltech

![CI](https://github.com/tagne200/bigdata-globaltech/actions/workflows/ci.yml/badge.svg)

Prototype de plateforme Big Data pour GlobalTech Industries : collecte, nettoyage, historisation et analyse des données logistiques et industrielles sur **Hadoop (HDFS, YARN)** et **Apache Spark 3.5**, en conteneurs **Docker**.

**Équipe** : TAMO FOTSO Michel Darryl, TAGNE Kenneth Nathan, NGO NGWA Suzanne.

## Architecture

Sources (commandes, produits, clients, logs web, capteurs) → **bronze** (Parquet brut) → **silver** (typé, contrôlé, SCD2, pseudonymisé) → **gold** (modèle en étoile, indicateurs), avec une table de **rejets** et une table d'**audit**. Les capteurs passent par un dossier d'atterrissage lu en Structured Streaming.

Le détail est dans le document de cadrage (lot 0).

## Arborescence

| Dossier | Contenu |
|---|---|
| `docker/` | Docker Compose du cluster et configuration Hadoop (lot 1) |
| `src/bigdata/` | Code : `common`, `bronze`, `silver`, `gold`, `streaming`, `mapreduce` |
| `conf/` | Configuration externalisée (`app.json`) |
| `tests/` | Tests pytest |
| `scripts/` | Scripts d'exploitation (smoke test, HDFS, simulation de panne) |
| `tools/` | `stream_replay.py` fourni par l'enseignant |
| `docs/` | ADR, architecture, runbook, dictionnaire, benchmark, fiche d'incident |
| `data/` | Données fournies, **non versionnées** (voir ci-dessous) |

## Démarrage en développement

```bash
python -m venv .venv
# Windows : .venv\Scripts\activate   |   Linux/macOS : source .venv/bin/activate
pip install -r requirements-dev.txt
ruff check . && pytest
```

Java 17 doit être installé pour exécuter les tests Spark en local.

Les données ne sont pas dans Git : copiez l'échantillon fourni dans `data/echantillon/J1/`.

Le démarrage du cluster (moins de 10 minutes) sera décrit ici au lot 1.

## Contribuer

Voir [CONTRIBUTING.md](CONTRIBUTING.md) : une branche par ticket Jira, une PR revue par un autre membre, CI verte avant fusion.

## Documentation

- [Kit et dictionnaire des fichiers bruts fournis par l'enseignant](docs/kit-enseignant.md)
- [Modèle d'ADR](docs/adr/000-modele.md)
