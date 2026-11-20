#!/usr/bin/env bash
# Preuve du rejeu sans doublon de la couche bronze (lot 2, BG-25).
#
# Lance deux fois de suite l'ingestion bronze d'une même livraison et compare
# les comptages. Le rejeu est sans doublon si, après la 2e exécution, chaque
# table bronze contient exactement le même nombre de lignes qu'après la 1re.
# Le résultat est écrit dans docs/preuves/BG-25-rejeu-<livraison>.md (pour le rapport).
#
# Prérequis : la livraison est déjà déposée (scripts/deposer_livraison.sh).
# Usage, depuis PowerShell à la racine du dépôt :
#   docker compose exec spark-client bash scripts/preuve_rejeu_bronze.sh J1

set -euo pipefail
cd "$(dirname "$0")/.."

LIVRAISON="${1:-J1}"
RAPPORT="docs/preuves/BG-25-rejeu-${LIVRAISON}.md"
TEMP=$(mktemp -d)
mkdir -p docs/preuves

for RUN in 1 2; do
  echo "== Exécution ${RUN}/2 de l'ingestion bronze ${LIVRAISON} (sur YARN)"
  DEBUT=$(date +%s)
  spark-submit src/bronze/ingestion.py --livraison "${LIVRAISON}" 2>&1 | tee "${TEMP}/run${RUN}.log" | grep -E "BILAN|ERROR" || true
  echo "$(( $(date +%s) - DEBUT ))" > "${TEMP}/duree${RUN}"
  grep -o "BILAN .*" "${TEMP}/run${RUN}.log" | sed 's/^BILAN //' > "${TEMP}/bilan${RUN}.txt"
  hdfs dfs -count /data/bronze/* > "${TEMP}/hdfs${RUN}.txt"
done

if [ ! -s "${TEMP}/bilan1.txt" ]; then
  echo "Aucune ligne BILAN : l'ingestion a échoué, voir ${TEMP}/run1.log" >&2
  exit 1
fi

if diff -q "${TEMP}/bilan1.txt" "${TEMP}/bilan2.txt" >/dev/null; then
  VERDICT="REJEU SANS DOUBLON : comptages identiques après les 2 exécutions"
  CODE=0
else
  VERDICT="ECHEC : les comptages diffèrent entre les 2 exécutions"
  CODE=1
fi

{
  echo "# Preuve du rejeu sans doublon : livraison ${LIVRAISON} (BG-25)"
  echo
  echo "- **Date** : $(TZ=Europe/Paris date '+%Y-%m-%d %H:%M') (heure de Paris)"
  echo "- **Commande** : \`bash scripts/preuve_rejeu_bronze.sh ${LIVRAISON}\` (2 exécutions de \`src/bronze/ingestion.py\` sur YARN)"
  echo "- **Durées** : exécution 1 = $(cat "${TEMP}/duree1") s, exécution 2 = $(cat "${TEMP}/duree2") s"
  echo "- **Verdict** : **${VERDICT}**"
  echo
  echo "| Source | Lignes lues | Dont mal formées (gardées) | Total table après exécution 1 | Total table après exécution 2 |"
  echo "|---|---|---|---|---|"
  paste -d' ' "${TEMP}/bilan1.txt" "${TEMP}/bilan2.txt" | awk '{
    for (i = 1; i <= NF; i++) { split($i, kv, "="); if (i <= NF/2) a[kv[1]] = kv[2]; else b[kv[1]] = kv[2] }
    printf "| %s | %s | %s | %s | %s |\n", a["source"], a["lues"], a["mal_formees"], a["total_table"], b["total_table"]
  }'
  echo
  echo "Contre-exemple (mesuré sur l'échantillon J1, voir docs/adr/003-ingestion-bronze.md) : avec"
  echo "\`mode(\"append\")\`, la 2e exécution double la table des commandes (101 200 -> 202 400 lignes)."
  echo
  echo "Détail HDFS après l'exécution 2 (\`hdfs dfs -count\` : dossiers, fichiers, octets, chemin) :"
  echo
  echo '```'
  cat "${TEMP}/hdfs2.txt"
  echo '```'
} > "${RAPPORT}"

echo
echo "${VERDICT}"
echo "Preuve écrite dans ${RAPPORT}"
exit "${CODE}"
