# Image commune à tous les services du cluster GlobalTech (lot 1, BG-18).
#
# Une seule image pour NameNode, DataNodes, ResourceManager, NodeManagers,
# client Spark et Spark History Server : la même version de Java (17) et de
# Python (3.10) partout, ce qui évite les erreurs PySpark sur YARN.
# Justification complète : docs/adr/001-images-docker.md

# Image de base officielle Apache : Ubuntu 22.04, Java 17 (Temurin), Python 3,
# utilisateur "hadoop" et script de démarrage qui génère la configuration
# Hadoop à partir des variables d'environnement (docker/hadoop.env).
FROM apache/hadoop-runner:jdk17-u2204

ARG HADOOP_VERSION=3.5.0
ARG SPARK_VERSION=3.5.3
ARG HADOOP_URL=https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz
ARG SPARK_URL=https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz

# Hadoop 3.5.0
RUN sudo rm -rf /opt/hadoop \
 && sudo curl -fLSs -o /tmp/hadoop.tar.gz "${HADOOP_URL}" \
 && sudo tar -xzf /tmp/hadoop.tar.gz -C /opt \
 && sudo rm /tmp/hadoop.tar.gz \
 && sudo mv /opt/hadoop-${HADOOP_VERSION} /opt/hadoop \
 && sudo rm -rf /opt/hadoop/share/doc \
 && sudo chown -R hadoop:users /opt/hadoop

# Spark 3.5.3 (distribution compatible Hadoop 3)
RUN sudo curl -fLSs -o /tmp/spark.tgz "${SPARK_URL}" \
 && sudo tar -xzf /tmp/spark.tgz -C /opt \
 && sudo rm /tmp/spark.tgz \
 && sudo mv /opt/spark-${SPARK_VERSION}-bin-hadoop3 /opt/spark \
 && sudo chown -R hadoop:users /opt/spark

# Dossier des données HDFS (monté sur un volume Docker par service)
# et dossier du projet (monté depuis le PC).
RUN sudo mkdir -p /data/hdfs /opt/project \
 && sudo chown hadoop:users /data/hdfs /opt/project

ENV HADOOP_HOME=/opt/hadoop \
    SPARK_HOME=/opt/spark \
    PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3 \
    PATH=${PATH}:/opt/hadoop/sbin:/opt/spark/bin

WORKDIR /opt/hadoop
USER hadoop
