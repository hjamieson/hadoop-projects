# Spark HBase Tools
Utility programs used for managing HBase tables using Spark.

## Examples
| class | description |
|---|---|
| RowCounter | counts the rows in a table.  You can pass it start/end keys to limit the regions to scan.|
| RowPrefixCounter| counts the rows whose rowkey matches the given prefix.|
| TableReader | example of reading columns and writing them to HDFS.|