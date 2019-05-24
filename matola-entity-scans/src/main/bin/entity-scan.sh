#!/usr/bin/env bash
# run matolas entity table scan

if [ $# -eq 0 ]; then echo "no args given"; exit 1; fi

CLASS=org.oclc.matola.ld.EntityScan
RUNJAR=works-phat.jar

JARS=$(hbase mapredcp | tr ':' ',')

export HADOOP_CONF_DIR=/etc/hbase/conf
export YARN_CONF_DIR=/etc/hadoop/conf

spark2-submit --master yarn \
   --conf spark.yarn.queue=wideload \
   --deploy-mode cluster \
   --class $CLASS \
   --executor-memory 512m \
   --jars $JARS \
   $RUNJAR $@
