#!/usr/bin/env bash
#
# description:
#    scans Worldcat table and counts the number of holdings for each item
#    in the XWC collection (bibs).  You can use the -s and -e options to
#    control how much of the table to scan.  By default, it scans the entire
#    table.
#
CLAZZ=org.oclc.hbase.perf.spark.job.HoldingsScan
JAR=hbase-perf-spark-0.1-beta-fat.jar
LIBS=$(hbase mapredcp | tr ':' ',')
export HADOOP_CONF_DIR=/etc/hbase/conf
export YARN_CONF_DIR=/etc/hadoop/conf

if [ $# -eq 0 ]; then echo "Usage: $0  [-s <startkey>] [-e <endkey>] -o <output-dir>"; exit 1; fi

spark2-submit --master yarn \
  --conf spark.yarn.submit.waitAppCompletion=false \
  --deploy-mode cluster \
  --num-executors 25 \
  --executor-memory 1536m \
  --executor-cores 3 \
  --class $CLAZZ \
  --jars $LIBS \
  $JAR $@