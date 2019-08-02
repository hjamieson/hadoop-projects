#!/usr/bin/env bash
# run matolas columnsize scan

if [ $# -eq 0 ]; then echo "no args given"; exit 1; fi

CLASS=org.oclc.matola.ld.columnsize.Driver
RUNJAR=matola-phat.jar

JARS=$(hbase mapredcp | tr ':' ',')
echo $JARS

spark2-submit --master local \
   --class $CLASS \
   --num-executors 20 \
   --executor-memory 1024m \
   --jars $JARS \
   $RUNJAR $@
