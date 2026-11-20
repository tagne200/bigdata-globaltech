#!/usr/bin/env bash
# Arborescence HDFS du projet GlobalTech (lot 2, BG-23).
# Justification : docs/adr/002-arborescence-hdfs.md
#
# Idempotent : "mkdir -p" ne fait rien si le dossier existe déjà. Le script peut
# donc être relancé autant de fois que nécessaire, sans erreur ni effet de bord.
#
# Lancé automatiquement à chaque "docker compose up" par le service hdfs-init.
# Lancement manuel possible, depuis PowerShell à la racine du dépôt :
#   docker compose exec spark-client bash scripts/hdfs_arborescence.sh
#
# Les droits par groupe et les quotas sont posés au ticket BG-26.

set -euo pipefail

DOSSIERS=(
  /data/landing/batch      # livraisons J1, J2, J3 copiées telles quelles (CSV, logs)
  /data/landing/capteurs   # fichiers JSONL déposés par tools/stream_replay.py (lot 5)
  /data/bronze             # brut en Parquet, partitionné par ingest_date (lot 2)
  /data/silver             # typé, dédupliqué, historisé, pseudonymisé (lot 3)
  /data/rejects            # lignes rejetées avec leurs motifs (lot 3)
  /data/gold               # modèle en étoile et indicateurs (lot 4)
  /data/audit/runs         # table d'audit des exécutions (lot 8)
  /checkpoints/capteurs    # état du streaming, pour la reprise après arrêt (lot 5)
)

hdfs dfs -mkdir -p "${DOSSIERS[@]}"

echo "Arborescence HDFS en place :"
hdfs dfs -ls -R /data /checkpoints | grep '^d' | awk '{print "  " $NF}'
