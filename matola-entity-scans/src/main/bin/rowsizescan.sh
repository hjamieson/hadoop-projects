#!/usr/bin/env bash
# run matolas rowsize scan

if [ $# -eq 0 ]; then echo "no args given"; exit 1; fi

CLASS=org.oclc.matola.ld.rowsize.RowSizeScan
RUNJAR=matola-phat.jar

JARS=$(hbase mapredcp | tr ':' ',')
echo $JARS

spark2-submit --master local \
   --class $CLASS \
   --executor-memory 512m \
   --jars $JARS \
   $RUNJAR $@
