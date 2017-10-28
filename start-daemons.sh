
if [ -f "/home/acadgild/project/logs/current-batch.txt"]
then
echo "Batch FileFound"

else 
echo -n "1" > "/home/acadgild/project/logs/current-batch.txt"
if
chmod 775 /home/acadgild/project/logs/current-batch.txt
batchid = cat /home/acadgild/project/logs.current-batch.txt
LOGFILE = /home/acadgild/project/logs/log_batch_$batchid

echo "Starting daemons" >> $LOGFILE
start-all.sh
start-hbase.shmr-jobhistory-daemon.sh start historyserver

sh /home/acadgild/project/scripts/populate-lookup.sh

sh /home/acadgild/project/scripts/dataformatting.sh

spark-shell