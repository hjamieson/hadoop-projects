#!/usr/bin/env bash
# run matolas table scan

if [ $# -eq 0 ]; then echo "no args given"; exit 1; fi

CLASS=org.oclc.matola.ld.ScanTable
RUNJAR=works-scans.jar

JARS=$(hbase mapredcp | tr ':' ',')
echo $JARS

spark2-submit --master local \
   --class $CLASS \
   --executor-memory 512m \
   --jars $JARS \
   $RUNJAR $@
