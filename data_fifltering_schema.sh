#!/bin/bash

batchid = cat /home/acadgild/project/logs/current-batch.txt
LOGFILE = /home/acadgild/project/logs/log_batch_$batchid

echo 'Creating hive tables on top of hbase for data enrichment'

hive - f /home/acadgild/project/scripts/create_hive_hbase_lookup.hql
