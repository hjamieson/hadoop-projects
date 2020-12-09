# Extraction Notes

## Region metrics in json mbean
Metrics for each region on this RS is found here:
```json
{
            "name": "Hadoop:service=HBase,name=RegionServer,sub=Regions",
            "modelerType": "RegionServer,sub=Regions",
            "tag.Context": "regionserver",
            "tag.Hostname": "hddev1db014dxc1.dev.oclc.org"
}
```

## Asking JMX For Stuff
Valid Forms(as documented in `org.apache.hadoop.hbase.http.jmx.JMXJsonServlet`)
1. returns the whole enchilada
`http http://hddev1db014dxc1.dev.oclc.org:60030/jmx` 
1. rs accepts a `qry` param that queries a subset of beans:
`http://hddev1db014dxc1.dev.oclc.org:60030/jmx?qry=Hadoop:*` 
1. query for a specific mbean:  
`http://hddev1db014dxc1.dev.oclc.org:60030/jmx\?qry=Hadoop:service=HBase,name=RegionServer,sub=Server` 
1. query for the info-by-region metrics:
`http://hddev1db014dxc1.dev.oclc.org:60030/jmx\?qry=Hadoop:service=HBase,name=RegionServer,sub=Regions`  


## ElasticSearch
1. docker pull docker.elastic.co/elasticsearch/elasticsearch:7.9.3
1. docker run -p 9200:9200 -p 9300:9300 -e "discovery.type=single-node" docker.elastic.co/elasticsearch/elasticsearch:7.9.3
1. hbase index template:  
   ```
   PUT /_index_template/hbase-template
   {
       "index_patterns":"hbase*",
       "template": {
           "settings": {
               "number_of_shards": 1
           },
           "mappings": {
               "properties": {
                   "@timestamp": {
                       "type": "date",
                       "format":"epoch_millis"
                   }
               }
           }
       }
   }
   ```
   
   ## Bulk Loading
   The job Poll4Bulk polls the RSs for data and writes each result to a file in ELK bulk-load format, suitable for use with Curl or other posting tools.
   * bulk format is:  
   ```
   POST _bulk
   { "index" : { "_index" : "test", "_id" : "1" } }
   { "field1" : "value1" }
   { "delete" : { "_index" : "test", "_id" : "2" } }
   { "create" : { "_index" : "test", "_id" : "3" } }
   { "field1" : "value3" }
   { "update" : {"_id" : "1", "_index" : "test"} }
   { "doc" : {"field2" : "value2"} }
   ```
   * _note_: normal gateways do not have access the rs ports like 60030, so make sure you run this on a admin ge!
   * to run it:
   ```
    $ java -cp jmx-extractor-0.1.jar:$(hbase classpath) org.oclc.hbase.tools.extractor.job.Poll4Bulk
   ```
   