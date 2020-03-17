#!/usr/bin/env bash
CLAZZ=org.oclc.hbase.spark.job.HoldingsScan
JAR=spark-hbase-tools-assembly-0.1.jar
LIBS=$(hbase mapredcp | tr ':' ',')
export HADOOP_CONF_DIR=/etc/hbase/conf
export YARN_CONF_DIR=/etc/hadoop/conf

if [ $# -eq 0 ]; then echo "Usage: $0  [-s <startkey>] [-e <endkey>] -o <output-dir>"; exit 1; fi

spark2-submit --master yarn \
  --deploy-mode cluster \
  --num-executors 25 \
  --executor-memory 1536m \
  --executor-cores 3 \
  --class $CLAZZ \
  --jars $LIBS \
  $JAR $@