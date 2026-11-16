# bigdata-globaltech

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

## Démarrage

Les données ne sont pas dans Git : copiez l'échantillon fourni par l'enseignant dans `data/echantillon/J1/`.

Le démarrage du cluster (moins de 10 minutes) sera décrit ici au lot 1, et les outils de vérification du code (tests, CI) au lot 8.

## Contribuer

Voir [CONTRIBUTING.md](CONTRIBUTING.md) : une branche par ticket Jira, une PR revue par un autre membre avant fusion.

## Documentation

- [Kit et dictionnaire des fichiers bruts fournis par l'enseignant](docs/kit-enseignant.md)
- [Modèle d'ADR](docs/adr/000-modele.md)
