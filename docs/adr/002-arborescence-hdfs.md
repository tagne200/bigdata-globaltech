# ADR-002 : arborescence HDFS et zone d'atterrissage

- **Statut** : proposé (à valider par l'équipe lors de la revue de la PR)
- **Date** : 20 novembre 2026
- **Auteur** : TAMO FOTSO Michel Darryl
- **Ticket Jira** : BG-23
- **Script** : `scripts/hdfs_arborescence.sh` (lancé automatiquement par le service `hdfs-init`)

## Contexte

L'énoncé demande une « arborescence HDFS justifiée » avec les couches bronze, silver, gold et une table de rejets, plus un flux de capteurs, un audit des exécutions et des droits et quotas (BG-26). L'arborescence doit être identique sur les 3 PC de l'équipe et recréée sans action manuelle.

Contrainte technique découverte en préparant l'ingestion : sur YARN, ce sont les **NodeManagers** qui lisent les fichiers d'entrée. Seul le conteneur `spark-client` voit le dossier `data/` du PC (monté dans `/opt/project`). Un `spark.read.csv("/opt/project/data/...")` lancé sur YARN échoue donc côté exécuteurs.

## Décision

```
/data
├── landing
│   ├── batch/<J1|J2|J3>/<source>/     livraisons brutes copiées depuis data/ (CSV, logs)
│   └── capteurs/                      fichiers JSONL déposés par stream_replay.py (lot 5)
├── bronze/<source>/ingest_date=AAAA-MM-JJ/   brut en Parquet (lot 2)
├── silver/<table>/                    données propres et historisées (lot 3)
├── rejects/<source>/livraison=Jx/     lignes rejetées avec motifs (lot 3)
├── gold/<table>/                      modèle en étoile et indicateurs (lot 4)
└── audit/runs/                        table d'audit des exécutions (lot 8)
/checkpoints/capteurs/                 état du streaming (lot 5)
/spark-logs/                           historique Spark (lot 1, déjà en place)
```

Le script est **idempotent** (`hdfs dfs -mkdir -p`). Il est relancé à chaque `docker compose up` par `hdfs-init`, sans risque.

**Ajout par rapport au fichier de contexte** : le dossier `/data/landing/batch`. La section 5 de `CONTEXTE_PROJET.md` ne prévoyait que `/data/landing/capteurs`. Cet ajout doit donc être validé par l'équipe puis reporté dans le fichier de contexte.

## Justification, chiffres à l'appui

**1. Un dossier racine par couche.** Les droits (BG-26) se posent par dossier dans HDFS. Avec une racine par couche, une seule commande suffit par règle, par exemple « le groupe `analyst` lit `/data/gold` mais pas `/data/silver` ». Un quota d'espace se pose aussi sur un seul dossier (`hdfs dfsadmin -setSpaceQuota`).

**2. Partition de bronze par date d'ingestion, pas par date de commande.** Sur l'échantillon J1, le profilage (`docs/profilage-J1.md`) compte 517 dates hors du mois livré et 44 dates illisibles (`N/A`, `hier`, vide). Partitionner le brut par date de commande obligerait à interpréter les dates avant de stocker, ce qui contredit le principe du brut sans perte. Côté taille : la livraison complète J1 fera environ 4 millions de commandes, soit **environ 350 Mo de CSV** (8,75 Mo mesurés pour 100 000 lignes, × 40). En Parquet, ça donne **environ 87 Mo** (rapport de 4 mesuré, voir ADR-003). Avec une partition par jour de commande, on aurait 31 partitions d'environ 2,8 Mo par livraison, soit 45 fois moins qu'un bloc HDFS de 128 Mo : c'est le problème des « petits fichiers ». Avec une partition par livraison, on garde des fichiers de taille raisonnable.

**3. Zone d'atterrissage batch dans HDFS plutôt qu'un montage du dossier `data/` dans les NodeManagers.**

| Option | Pour | Contre |
|---|---|---|
| A. Monter `./data` dans les 2 NodeManagers | Aucune copie | Ne marche que parce que tous les conteneurs tournent sur le même PC : sur un vrai cluster, les nœuds n'ont pas de dossier commun. Aucune localité des données. |
| **B. Copier la livraison dans `/data/landing/batch` (retenue)** | Démarche standard d'un data lake (zone d'atterrissage, puis bronze). Les exécuteurs lisent des blocs HDFS locaux. Le chemin ne dépend pas du PC. | Une copie de plus : environ 19 Mo pour l'échantillon, environ 350 Mo de CSV de commandes par livraison complète, × 3 répliques = environ 1 Go physique |

Le coût de B reste faible face aux 15 Go de disque demandés par le guide de démarrage. Comme le bronze garde tout (ADR-003), le landing batch peut être purgé une fois l'ingestion validée. Ce volume est un bon cas pour la comparaison entre réplication et erasure coding (BG-52).

**4. Landing capteurs isolé.** Le flux dépose 1 fichier toutes les 5 s, soit 720 par heure et **17 280 par jour**. Chaque fichier occupe environ 300 octets dans la mémoire du NameNode (environ 150 octets pour l'objet fichier et autant pour son bloc), soit environ 5 Mo de tas par jour de flux, pour un NameNode limité à 512 Mo (`HADOOP_HEAPSIZE_MAX`). Isoler ce dossier permet d'y poser un **quota de noms** (BG-26), qui borne le nombre de fichiers sans gêner les autres couches.

**5. Checkpoints hors de `/data`.** Le checkpoint du streaming est de l'état technique de Spark, pas une donnée métier. Il ne doit jamais être lu par un job batch ni compté dans les quotas des couches.

## Conséquences

- Après `docker compose up -d`, l'arborescence existe chez chacun, sans commande supplémentaire.
- Une livraison s'ingère en 2 étapes : dépôt en landing (`scripts/deposer_livraison.sh`), puis ingestion bronze (`src/bronze/ingestion.py`). Voir `docs/bronze.md`.
- Les droits, les groupes et les quotas sont traités dans BG-26, sur cette arborescence.
