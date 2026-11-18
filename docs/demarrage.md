# Démarrer le cluster GlobalTech

Ce guide démarre le cluster Hadoop + Spark sur un PC Windows. Objectif : **moins de 10 minutes** une fois Docker Desktop installé et l'image construite.

## Prérequis (une seule fois)

| Élément | Détail |
|---|---|
| PC | Windows 10 ou 11, **16 Go de RAM conseillés** (8 Go possible, voir « Petit PC » plus bas), 15 Go de disque libre |
| Docker Desktop | https://www.docker.com/products/docker-desktop, avec WSL 2 |
| Git | https://git-scm.com/download/win |
| Mémoire donnée à Docker | Fichier `C:\Users\<vous>\.wslconfig` contenant `[wsl2]` puis `memory=10GB` (6GB sur un PC de 8 Go), puis `wsl --shutdown` |

## Démarrage

Dans PowerShell, à la racine du dépôt :

```powershell
# 1. Récupérer la dernière version du code
git switch main
git pull

# 2. Construire l'image (première fois : 5 à 15 min ; ensuite : quelques secondes)
docker compose build

# 3. Démarrer les 9 conteneurs
docker compose up -d

# 4. Vérifier que tout fonctionne (environ 2 min)
docker compose exec spark-client bash scripts/smoke_test.sh
```

Le test doit finir par **`SMOKE TEST OK`**.

## Interfaces web

| Adresse | Ce qu'on doit y voir |
|---|---|
| http://localhost:9870 | HDFS : onglet **Datanodes**, 3 nœuds « In service » |
| http://localhost:8088 | YARN : **Active Nodes = 2** |
| http://localhost:18080 | Historique Spark : le job `PythonPi` |

## Utilisation

```powershell
# Ouvrir un terminal dans le client Spark (le dépôt est dans /opt/project)
docker compose exec spark-client bash

# Ouvrir une console PySpark connectée à YARN depuis ce terminal
pyspark
```

## Arrêt

```powershell
docker compose down      # arrête le cluster, les données HDFS sont conservées
docker compose down -v   # arrête ET efface toutes les données HDFS (remise à zéro)
```

## Petit PC (8 Go de RAM)

Dans `docker/hadoop.env`, remplacer `yarn.nodemanager.resource.memory-mb=2048` par `1536`, et dans `docker/spark-defaults.conf` mettre `spark.executor.instances 1`. Ne pas commiter ce changement.

## En cas de problème

| Symptôme | Cause probable | Solution |
|---|---|---|
| `port is already allocated` | Un autre programme utilise 9870, 8088 ou 18080 | Fermer ce programme, ou `docker compose down` puis relancer |
| Un conteneur redémarre en boucle | Voir son journal | `docker compose logs namenode` (remplacer par le nom du conteneur) |
| `$'\r': command not found` | Fins de ligne Windows dans un script | Supprimer le dossier du dépôt et le recloner (le fichier `.gitattributes` impose les bonnes fins de ligne) |
| Le job Spark reste « ACCEPTED » | Mémoire YARN insuffisante | Vérifier http://localhost:8088 ; appliquer « Petit PC » |
| `docker compose build` échoue au téléchargement | Connexion coupée | Relancer `docker compose build` |
| `hdfs-init` est « Exited (0) » | Normal : il prépare HDFS puis s'arrête | Rien à faire |
