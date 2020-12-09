#!/bin/bash
while true; do
   curl -s http://elasticsearch:9200/
   if [ $? = 0 ]; then break; fi
   echo "$(date +%T) waiting for elk"
   sleep 10s
done
echo "elk is ready"
/usr/app/setup/add-templates.sh