#!/usr/bin/env bash
CLAZZ=org.oclc.hbase.tools.hbase.TableReader
#JAR=table-reader_2.11-0.1.jar
JAR=table-reader-assembly-0.1.jar
LIBS=$(org.oclc.hbase.tools.hbase mapredcp | tr ':' ',')
export HADOOP_CONF_DIR=/etc/org.oclc.hbase.tools.hbase/conf
export YARN_CONF_DIR=/etc/hadoop/conf

spark2-submit --master yarn \
  --deploy-mode cluster \
  --num-executors 1 \
  --executor-memory 512m \
  --class $CLAZZ \
  --jars $LIBS \
  $JAR $@