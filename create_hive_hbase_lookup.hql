

create external table if not exists station_geo_map
(
station_id String,
geo_cd String
) 
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
with serdeproperties
("hbase.columns.mapping" = ":key,geo:geo_cd");


create external table if not exists subscribed_users
(
user_id String,
subscn_start_dt String,
subscn_end_dt String
) 
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
with serdeproperties
("hbase.columns.mapping" = ":key,subscn:startdt,subscn:enddt");
tblproperties("hbase.table.name" = "subscribed-users")

create external table if not exists song_artist_map
(
song_id String,
artist_id String
) 
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
with serdeproperties
("hbase.columns.mapping" = ":key,artist:artistid);
tblproperties("hbase.table.name" = "song-artist-map")