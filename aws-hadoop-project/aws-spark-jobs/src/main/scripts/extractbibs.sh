#!/usr/bin/env bash
#
# extracts bibs from Worldcat and marshals to simple JSON objects.
#
# example: ./extractbibs.sh -t Worldcat -s 1 -e 10026969 -o bibs.json -r 10
#
CLAZZ=code.aws.DumpWorldcatRecordsWithRepartition
JAR=aws-spark-jobs-0.1-phat.jar
LIBS=$(hbase mapredcp | tr ':' ',')
export HADOOP_CONF_DIR=/etc/hbase/conf
export YARN_CONF_DIR=/etc/hadoop/conf

# --deploy-mode cluster \

spark2-submit --master yarn \
  --deploy-mode cluster \
  --num-executors 1 \
  --executor-memory 1024m \
  --class $CLAZZ \
  --jars $LIBS \
  $JAR $@