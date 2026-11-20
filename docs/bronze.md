# Couche bronze : charger une livraison

Ce guide charge une livraison (J1, J2 ou J3) dans la couche bronze, sur le cluster. Il suppose que le cluster est démarré (`docs/demarrage.md`).
Les décisions sont expliquées dans `docs/adr/002-arborescence-hdfs.md` (arborescence) et `docs/adr/003-ingestion-bronze.md` (format, idempotence).

## Vue d'ensemble

```
data/<livraison>/ (PC)  --deposer_livraison.sh-->  /data/landing/batch/<livraison>/  --ingestion.py (YARN)-->  /data/bronze/<source>/ingest_date=.../
```

## Étapes (PowerShell, à la racine du dépôt)

```powershell
# 0. Une seule fois : copier les données du kit de la prof dans data/ (jamais versionné)
robocopy "C:\CHEMIN\VERS\Kit_BigData_Spark_Etudiants_v3\skeleton\data" "data" /E

# 1. Démarrer le cluster (l'arborescence HDFS est créée automatiquement par hdfs-init)
docker compose up -d

# 2. Déposer la livraison dans HDFS (échantillon J1 ; pour les vraies livraisons : ... J1  sans 2e argument, qui lit data/J1)
docker compose exec spark-client bash scripts/deposer_livraison.sh J1 data/echantillon/J1

# 3. Ingérer en bronze (le job part sur YARN)
docker compose exec spark-client spark-submit src/bronze/ingestion.py --livraison J1

# 4. (BG-25) Prouver le rejeu sans doublon : 2 exécutions, comparaison des comptages
docker compose exec spark-client bash scripts/preuve_rejeu_bronze.sh J1
```

## Résultat attendu avec l'échantillon J1

```
BILAN source=commandes ... lues=101200 mal_formees=56 ecrites=101200 total_table=101200
BILAN source=produits  ... lues=592    mal_formees=0  ecrites=592    total_table=592
BILAN source=clients   ... lues=5127   mal_formees=0  ecrites=5127   total_table=5127
BILAN source=logs_web  ... lues=50000  mal_formees=0  ecrites=50000  total_table=50000
```

Vérification visuelle : http://localhost:9870, menu **Utilities > Browse the file system**, dossier `/data/bronze`.

## Lire la couche bronze dans un autre job (lot 3 et suivants)

```python
commandes = (spark.read
             .option("mergeSchema", True)      # nécessaire si une livraison a ajouté une colonne
             .parquet("/data/bronze/commandes")
             .where("ingest_date = '2026-02-01'"))   # une seule livraison
# Toutes les colonnes métier sont en texte (StringType) : le typage se fait en silver.
# Lignes mal formées : commandes.where("_corrupt_record IS NOT NULL")
```

## En cas de problème

| Symptôme | Cause probable | Solution |
|---|---|---|
| `Dossier introuvable : data/J1` | Données du kit pas copiées, ou échantillon utilisé sans 2e argument | Étape 0, ou ajouter `data/echantillon/J1` |
| `Path does not exist: hdfs://namenode:8020/data/landing/batch/J1/...` | Étape 2 oubliée | Lancer `deposer_livraison.sh` |
| Le job reste « ACCEPTED » dans http://localhost:8088 | Un autre job occupe YARN (une console `pyspark` ouverte, par exemple) | Fermer l'autre job, ou attendre qu'il finisse |
| `lues` différent de `ecrites`, code de sortie 1 | Écriture interrompue | Relancer l'étape 3 (sans risque : idempotent) |
