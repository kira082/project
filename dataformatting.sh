#!/bin/bash

batchid = cat /home/acadgild/project/logs/current-batch.txt
LOGFILE = /home/acadgild/project/logs/log_batch_$batchid

echo 'Placing data files from local to HDFS...' >> $LOGFILE

hadoop fs -rm -r /user/acadgild/hadoop/project/batch ${batchid}/web/
hadoop fs -rm -r /user/acadgild/hadoop/project/batch ${batchid}/formattedweb/
hadoop fs -rm -r /user/acadgild/hadoop/project/batch ${batchid}/mob/

hadoop fs -mkdir -p /user/acadgild/hadoop/project/batch ${batchid}/web/
hadoop fs -mkdir -p /user/acadgild/hadoop/project/batch ${batchid}/mob/

hadoop fs -put /home/acadgild/project/web /user/acadgild/hadoop/project/batch ${batchid}/web/
hadoop fs -put /home/acadgild/project/mob  /user/acadgild/hadoop/project/batch ${batchid}/mob/

echo 'Running pig script for data formatting '  >> $LOGFILE

pig -param batchid = $batchid /home/acadgild/project/script/dataformatting.pig

echo 'running hive script for formatted data load..' >> $LOGFILE

hive - hiveconf batchid = $batchid -f /home/acadgild/project/scripts/formatted_hivr_load.hql
