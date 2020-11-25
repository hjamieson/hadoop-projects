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