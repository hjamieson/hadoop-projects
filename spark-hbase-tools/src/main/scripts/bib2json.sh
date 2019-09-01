#!/usr/bin/env bash
CLAZZ=org.oclc.hbase.tools.hbase.Bib2Json
JAR=spark-org.oclc.hbase.tools.hbase-tools-assembly-0.1.jar
LIBS=$(org.oclc.hbase.tools.hbase mapredcp | tr ':' ',')
export HADOOP_CONF_DIR=/etc/org.oclc.hbase.tools.hbase/conf
export YARN_CONF_DIR=/etc/hadoop/conf

if [ $# -eq 0 ]; then echo "Usage: $0 -t <table> [-s <startkey>] [-e <endkey>] -o <output-dir>"; exit 1; fi

spark2-submit --master yarn \
  --deploy-mode cluster \
  --num-executors 1 \
  --executor-memory 512m \
  --class $CLAZZ \
  --jars $LIBS \
  $JAR $@