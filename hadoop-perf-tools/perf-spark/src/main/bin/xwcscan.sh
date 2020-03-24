#!/usr/bin/env bash
CLAZZ=org.oclc.hbase.perf.spark.job.XwcScan
JAR=hbase-perf-spark-0.1-beta-fat.jar
LIBS=$(hbase mapredcp | tr ':' ',')
export HADOOP_CONF_DIR=/etc/hbase/conf
export YARN_CONF_DIR=/etc/hadoop/conf
JOBQUEUE="default"

if [ $# -eq 0 ]; then echo "Usage: $0 [-s <startkey>] [-e <endkey>] -o <output-dir>"; exit 1; fi

spark2-submit --master yarn \
  --conf spark.yarn.submit.waitAppCompletion=false \
  --deploy-mode cluster \
  --num-executors 100 \
  --executor-cores 3 \
  --executor-memory 2g \
  --queue $JOBQUEUE \
  --class $CLAZZ \
  --jars $LIBS \
  $JAR $@