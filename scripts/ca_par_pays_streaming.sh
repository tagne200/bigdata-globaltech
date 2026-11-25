#!/usr/bin/env bash
# Chiffre d'affaires par pays avec Hadoop Streaming sur YARN (ticket BG-46).
#
# Usage (dans le conteneur spark-client) :
#     bash scripts/ca_par_pays_streaming.sh J1
#
# Hadoop Streaming lit des FICHIERS TEXTE : on lit donc les CSV déposés dans la zone
# d'atterrissage (landing), pas le Parquet du bronze. Le mapper et le reducer sont de simples
# scripts Python que Hadoop lance sur les NodeManagers (-files les y envoie).
set -euo pipefail

LIVRAISON="${1:?Usage : bash scripts/ca_par_pays_streaming.sh J1|J2|J3}"
ENTREE="/data/landing/batch/${LIVRAISON}/commandes"
SORTIE="/data/mapreduce/ca_par_pays/${LIVRAISON}"
JOURNAL="/tmp/mapreduce_ca_${LIVRAISON}.log"

HADOOP_HOME="${HADOOP_HOME:-/opt/hadoop}"
JAR="$(ls "${HADOOP_HOME}"/share/hadoop/tools/lib/hadoop-streaming-*.jar 2>/dev/null | head -1 || true)"
if [ -z "${JAR}" ]; then
  JAR="$(find / -name 'hadoop-streaming*.jar' -not -name '*sources*' 2>/dev/null | head -1 || true)"
fi
[ -n "${JAR}" ] || { echo "ERREUR : hadoop-streaming*.jar introuvable" >&2; exit 1; }

if ! hdfs dfs -test -d "${ENTREE}"; then
  echo "ERREUR : ${ENTREE} n'existe pas. Charger la livraison avec :" >&2
  echo "  bash scripts/deposer_livraison.sh ${LIVRAISON} data/echantillon/${LIVRAISON}" >&2
  exit 1
fi

# MapReduce REFUSE d'écrire dans un dossier de sortie qui existe déjà : on le supprime avant,
# ce qui rend le lancement rejouable (même résultat à chaque fois, rien ne s'accumule).
hdfs dfs -rm -r -f -skipTrash "${SORTIE}" > /dev/null 2>&1 || true

DEBUT=$(date +%s)
# Les options -D (génériques) doivent venir AVANT les options propres à Streaming.
# Mémoires réduites : le cluster a 2 NodeManagers de 2 Go ; sans cela, le conteneur de
# coordination (ApplicationMaster) réserve 1,5 Go et le job attend sa place.
hadoop jar "${JAR}" \
  -D mapreduce.job.name="ca-par-pays-${LIVRAISON}" \
  -D mapreduce.job.reduces=1 \
  -D yarn.app.mapreduce.am.resource.mb=512 \
  -D yarn.app.mapreduce.am.command-opts=-Xmx384m \
  -D mapreduce.map.memory.mb=512 \
  -D mapreduce.map.java.opts=-Xmx384m \
  -D mapreduce.reduce.memory.mb=512 \
  -D mapreduce.reduce.java.opts=-Xmx384m \
  -files src/mapreduce/mapper_ca.py,src/mapreduce/reducer_ca.py \
  -mapper "python3 mapper_ca.py" \
  -combiner "python3 reducer_ca.py" \
  -reducer "python3 reducer_ca.py --euros" \
  -input "${ENTREE}" \
  -output "${SORTIE}" 2>&1 | tee "${JOURNAL}"
FIN=$(date +%s)

# Le ticket exige une exécution sur YARN. Sans la bonne configuration, Hadoop peut lancer le job
# en mode LOCAL (identifiant job_local...) : il fonctionnerait, mais pas sur le cluster.
if grep -q "job_local" "${JOURNAL}"; then
  echo "ERREUR : le job a tourné en mode LOCAL, pas sur YARN (mapreduce.framework.name != yarn)." >&2
  exit 2
fi
APPLICATION="$(grep -o 'application_[0-9_]*' "${JOURNAL}" | head -1 || true)"
echo "APPLICATION_YARN=${APPLICATION:-introuvable}"

echo
echo "===== RÉSULTAT : pays <TAB> CA en euros <TAB> nombre de commandes ====="
hdfs dfs -cat "${SORTIE}"/part-*
echo
echo "===== COMPTEURS (lignes invalides, etc.) ====="
grep -E "^\s*(lignes_lues|lignes_valides|en_tetes|exclues_annulees|invalides_[a-z]+|pays_en_sortie)=" "${JOURNAL}" || true
echo
echo "===== MESURES POUR LA COMPARAISON (BG-48) ====="
grep -E "Map input records|Map output records|Combine input records|Combine output records|Reduce input records|Reduce output records|HDFS: Number of bytes read|Total time spent by all map tasks" "${JOURNAL}" || true
echo "DUREE_MAPREDUCE_S=$((FIN - DEBUT))"
