#!/bin/bash
echo "db.registeration_.find({"timestamp":{'\$lt':`date -d '10 minute ago' '+%s'`}}, {"_id":1})" | /opt/mongodb-linux-x86_64-2.4.5/bin/mongo 192.168.1.15:33458/CACHE | sed '1,2d' | sed '$d' | grep -v -e '^registeration_$' -e '^bye$' | awk -F '"' '{print $4}' | while read collection; do
    echo '------------------------------' $collection '------------------------------'
    echo "db.registeration_.remove({'_id':\"$collection\"})"
    echo "db.registeration_.remove({'_id':\"$collection\"})" | /opt/mongodb-linux-x86_64-2.4.5/bin/mongo 192.168.1.15:33458/CACHE
    echo "db[\"$collection\"].drop()" 
    echo "db[\"$collection\"].drop()" | /opt/mongodb-linux-x86_64-2.4.5/bin/mongo 192.168.1.15:33458/CACHE
    echo '------------------------------' $collection '------------------------------'
done