#!/usr/bin/env bash
CLAZZ=hbase.Bib2Json
JAR=spark-hbase-tools-assembly-0.1.jar
LIBS=$(hbase mapredcp | tr ':' ',')
export HADOOP_CONF_DIR=/etc/hbase/conf
export YARN_CONF_DIR=/etc/hadoop/conf

if [ $# -eq 0 ]; then echo "Usage: $0 -t <table> [-s <startkey>] [-e <endkey>] -o <output-dir>"; exit 1; fi

spark2-submit --master yarn \
  --deploy-mode cluster \
  --num-executors 1 \
  --executor-memory 512m \
  --class $CLAZZ \
  --jars $LIBS \
  $JAR $@