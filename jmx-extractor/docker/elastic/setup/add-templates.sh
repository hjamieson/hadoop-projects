#!/bin/bash
echo "$(date +%T) adding templates"

echo -e "\nadding hbase"
curl -s -XPUT -H "Content-Type: application/json" \
   http://elasticsearch:9200/_index_template/hbase-template \
   --data-binary "@/usr/app/setup/hbase-template.json"

echo -e "\nadding access-logs"
curl -s -XPUT -H "Content-Type: application/json" \
   http://elasticsearch:9200/_template/access-logs \
   --data-binary "@/usr/app/setup/access-logs-template.json"   

echo -e "\nadding orders"
curl -s -XPUT -H "Content-Type: application/json" \
   http://elasticsearch:9200/_template/orders \
   --data-binary "@/usr/app/setup/orders-template.json"   

echo -e "\n$(date +%T) templates installed"