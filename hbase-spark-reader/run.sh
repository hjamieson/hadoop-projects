#!/usr/bin/env bash
JARS=$(hbase mapredcp | tr ':' ',')
echo $JARS
spark-submit --master local[2] \
   --class example.ImportData \
  --jars $JARS \
  target/scala-2.11/hbase-spark-reader_2.11-0.1.0-SNAPSHOT.jar \
  spark-definitive/data/flight/2015-summary.json foo
