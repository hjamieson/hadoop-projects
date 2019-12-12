# Apache Flume Notes
## Flume Notes
### Running the examples
`nohup flume-ng agent -Xmx256m -f logtrap.conf -n a1 2>&1 &`

## Kafka Setup Notes
### Topic setup
* create topic:  
    `kafka-topics --zookeeper hddev1mb004dxc1.dev.oclc.org:2181/kafka --create --topic dbahadoop.log.collection --partitions 1 --replication-factor 2 --config retention.ms=30000`  
    > ttl = 30sec
* delete topic:  
    `kafka-topics --zookeeper hddev1mb004dxc1.dev.oclc.org:2181/kafka --delete --topic dbahadoop.log.collection`  
