#!/usr/bin/env bash
# Dépose une livraison (J1, J2 ou J3) dans la zone d'atterrissage HDFS (lot 2, BG-24).
#
# Pourquoi : sur YARN, ce sont les NodeManagers qui lisent les fichiers. Ils ne
# voient pas le dossier data/ du PC (seul spark-client le voit). Les fichiers
# bruts sont donc d'abord copiés dans HDFS, puis Spark les lit depuis HDFS.
# Voir docs/adr/002-arborescence-hdfs.md.
#
# Idempotent : le dossier de la source est supprimé puis recopié en entier.
# HDFS contient donc toujours une copie exacte du dossier local.
#
# Usage, depuis PowerShell à la racine du dépôt :
#   docker compose exec spark-client bash scripts/deposer_livraison.sh J1 data/echantillon/J1
#   docker compose exec spark-client bash scripts/deposer_livraison.sh J2      (lit data/J2)
#
# Les capteurs ne sont pas copiés ici : ils arrivent en flux via
# tools/stream_replay.py (lot 5).

set -euo pipefail
cd "$(dirname "$0")/.."

LIVRAISON="${1:-}"
case "${LIVRAISON}" in
  J1|J2|J3) ;;
  *) echo "Usage : bash scripts/deposer_livraison.sh J1|J2|J3 [dossier_local]" >&2; exit 2 ;;
esac
SOURCE_LOCALE="${2:-data/${LIVRAISON}}"
DESTINATION="/data/landing/batch/${LIVRAISON}"

if [ ! -d "${SOURCE_LOCALE}" ]; then
  echo "Dossier introuvable : ${SOURCE_LOCALE} (copier les données du kit dans data/, voir docs/bronze.md)" >&2
  exit 1
fi

hdfs dfs -mkdir -p "${DESTINATION}"
for SOURCE in commandes produits clients logs_web; do
  if [ ! -d "${SOURCE_LOCALE}/${SOURCE}" ]; then
    echo "  [ABSENT] ${SOURCE} : pas de dossier ${SOURCE_LOCALE}/${SOURCE}, ignoré"
    continue
  fi
  hdfs dfs -rm -r -f -skipTrash "${DESTINATION}/${SOURCE}" >/dev/null
  hdfs dfs -put "${SOURCE_LOCALE}/${SOURCE}" "${DESTINATION}/"

  # Contrôle : même nombre de fichiers en local et dans HDFS.
  NB_LOCAL=$(find "${SOURCE_LOCALE}/${SOURCE}" -maxdepth 1 -type f | wc -l)
  NB_HDFS=$(hdfs dfs -count "${DESTINATION}/${SOURCE}" | awk '{print $2}')
  if [ "${NB_LOCAL}" -ne "${NB_HDFS}" ]; then
    echo "  [ECHEC]  ${SOURCE} : ${NB_LOCAL} fichiers en local, ${NB_HDFS} dans HDFS" >&2
    exit 1
  fi
  echo "  [OK]     ${SOURCE} : ${NB_HDFS} fichier(s) -> ${DESTINATION}/${SOURCE}"
done

echo
echo "Volume déposé (taille logique, puis taille physique avec la réplication) :"
hdfs dfs -du -s -h "${DESTINATION}"
