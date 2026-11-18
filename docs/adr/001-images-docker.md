# ADR-001 : images Docker du cluster

- **Statut** : accepté
- **Date** : 19 novembre 2026
- **Auteur** : TAMO FOTSO Michel Darryl
- **Ticket Jira** : BG-17

## Contexte

L'énoncé impose un cluster Hadoop (HDFS + YARN) et Spark 3.5 sous Docker, sur les PC Windows de l'équipe. Le `docker-compose.yml` du kit utilise des images `bitnami`, qui ne sont plus publiées. L'équipe a retenu Hadoop 3.5.0 (première version stable de la ligne 3.5, avril 2026, conçue pour Java 17) et Spark 3.5.3 (même version que `pyspark==3.5.3` du kit).

Contrainte forte : avec PySpark sur YARN, le driver (conteneur client) et les exécuteurs (NodeManagers) doivent utiliser **la même version de Python**, sinon les jobs échouent.

## Options étudiées

| Option | Avantages | Inconvénients |
|---|---|---|
| A. Images `bitnami` du kit | Déjà écrites | Plus publiées : inutilisables |
| B. `apache/hadoop:3.5.0` + `apache/spark:3.5.3` | Images officielles prêtes | Deux images différentes : Java et Python différents entre client et NodeManagers ; au moment du choix, l'image Hadoop 3.5.0 posait encore des problèmes de version de Java côté Apache |
| C. **Une image commune** construite sur `apache/hadoop-runner:jdk17-u2204`, avec Hadoop 3.5.0 et Spark 3.5.3 téléchargés depuis les archives Apache | Java 17 et Python 3.10 identiques dans les 9 conteneurs ; versions exactes maîtrisées ; méthode identique à celle des images officielles Apache | Première construction plus longue (environ 1,5 Go à télécharger, une seule fois) |

## Décision

Option C : une image commune `globaltech/hadoop-spark:3.5.0-3.5.3`, définie dans `docker/Dockerfile`.

## Conséquences

- Tous les services démarrent à partir de la même image : un seul fichier à maintenir.
- La configuration est externalisée dans `docker/hadoop.env` et `docker/spark-defaults.conf`.
- La première commande `docker compose build` prend de 5 à 15 minutes selon la connexion ; les démarrages suivants prennent moins d'une minute.
- Risque suivi : Spark 3.5.3 embarque un client Hadoop 3.3 qui parle à un cluster 3.5.0. Le smoke test (`scripts/smoke_test.sh`) le vérifie à chaque démarrage ; en cas d'incompatibilité, repli sur Hadoop 3.4.x en changeant seulement `HADOOP_VERSION` dans le Dockerfile.
