#!/usr/bin/env bash
# Smoke test du cluster GlobalTech (lot 1, BG-21).
# Vérifie en environ 2 minutes que HDFS, YARN, Spark et l'historique Spark fonctionnent.
#
# Lancement depuis PowerShell, à la racine du dépôt :
#   docker compose exec spark-client bash scripts/smoke_test.sh

set -uo pipefail

ECHECS=0
DEBUT=$(date +%s)

ok()      { echo "  [OK]    $1"; }
echec()   { echo "  [ECHEC] $1"; ECHECS=$((ECHECS + 1)); }
etape()   { echo; echo "== $1"; }

etape "1/6 HDFS sorti du mode sans échec"
if hdfs dfsadmin -safemode wait >/dev/null 2>&1; then ok "NameNode prêt"; else echec "NameNode en mode sans échec"; fi

etape "2/6 Trois DataNodes vivants"
VIVANTS=$(hdfs dfsadmin -report 2>/dev/null | grep -oP 'Live datanodes \(\K[0-9]+' || echo 0)
if [ "${VIVANTS}" -eq 3 ]; then ok "3 DataNodes vivants"; else echec "${VIVANTS} DataNode(s) vivant(s) au lieu de 3"; fi

etape "3/6 Écriture HDFS répliquée 3 fois"
FICHIER=/tmp/smoke/test_$(date +%s).txt
hdfs dfs -mkdir -p /tmp/smoke 2>/dev/null
echo "smoke test GlobalTech $(date)" | hdfs dfs -put -f - "${FICHIER}" 2>/dev/null
if hdfs dfs -cat "${FICHIER}" >/dev/null 2>&1; then ok "écriture et relecture de ${FICHIER}"; else echec "écriture HDFS impossible"; fi
sleep 3
if hdfs fsck "${FICHIER}" -files -blocks -replicaDetails 2>/dev/null | grep -q "Live_repl=3"; then
  ok "bloc répliqué sur 3 DataNodes"
else
  echec "le bloc n'a pas 3 répliques (voir : hdfs fsck ${FICHIER} -files -blocks)"
fi

etape "4/6 Deux NodeManagers YARN actifs"
NOEUDS=$(yarn node -list 2>/dev/null | grep -oP 'Total Nodes:\K[0-9]+' || echo 0)
if [ "${NOEUDS}" -eq 2 ]; then ok "2 NodeManagers actifs"; else echec "${NOEUDS} NodeManager(s) au lieu de 2"; fi

etape "5/6 Job Spark (calcul de pi) sur YARN"
SORTIE=$(spark-submit --name smoke-test-pi "${SPARK_HOME}/examples/src/main/python/pi.py" 20 2>&1)
if echo "${SORTIE}" | grep -q "Pi is roughly"; then
  ok "$(echo "${SORTIE}" | grep "Pi is roughly")"
  APP_ID=$(echo "${SORTIE}" | grep -oP 'application_[0-9]+_[0-9]+' | head -1)
  [ -n "${APP_ID}" ] && ok "application YARN : ${APP_ID}"
else
  echec "le job Spark a échoué ; dernières lignes :"
  echo "${SORTIE}" | tail -20
fi

etape "6/6 Job visible dans le Spark History Server"
# On cherche l'identifiant YARN du job : son nom affiché est fixé par l'exemple
# pi.py lui-même ("PythonPi") et ne dépend donc pas de --name.
RECHERCHE="${APP_ID:-PythonPi}"
TROUVE=0
for _ in $(seq 1 12); do
  if curl -fs http://spark-history:18080/api/v1/applications 2>/dev/null | grep -q "${RECHERCHE}"; then TROUVE=1; break; fi
  sleep 5
done
if [ "${TROUVE}" -eq 1 ]; then
  ok "job ${RECHERCHE} présent sur http://localhost:18080"
else
  echec "job ${RECHERCHE} absent de l'historique Spark après 60 s (voir : docker compose logs spark-history)"
fi

DUREE=$(( $(date +%s) - DEBUT ))
echo
if [ "${ECHECS}" -eq 0 ]; then
  echo "SMOKE TEST OK : cluster opérationnel (${DUREE} s)"
  exit 0
else
  echo "SMOKE TEST EN ECHEC : ${ECHECS} contrôle(s) en échec (${DUREE} s)"
  exit 1
fi
