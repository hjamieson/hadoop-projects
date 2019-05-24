#!/usr/bin/env bash
CLAZZ=hbase.RowCounter
JAR=table-reader-assembly-0.1.jar
LIBS=$(hbase mapredcp | tr ':' ',')
export HADOOP_CONF_DIR=/etc/hbase/conf
export YARN_CONF_DIR=/etc/hadoop/conf

spark2-submit --master yarn \
  --deploy-mode cluster \
  --num-executors 1 \
  --executor-memory 512m \
  --class $CLAZZ \
  --jars $LIBS \
  $JAR $@