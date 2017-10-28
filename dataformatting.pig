REGISTER /home/acadgild/project/lib/piggbank.jar;

DEFINE XPATH org.apache.pig.piggybank.evaluation.xml.XPath();

A = LOAD '/user/acadgild/project/batch${batchid}/web/' using org.apache.pig.piggybank.storage.XMLLoader('record') as (x:chararray)

B = FOREACH A GENERATE TRIM (XPATH(x, 'record/user_id')) AS user_id'
TRIM (XPATH(x, 'record/song_id')) AS song_id'
TRIM (XPATH(x, 'record/artist_id')) AS artist_id'
ToUnixTime(ToDate(TRIM (XPATH(x, 'record/timestamp')), 'yyyy-MM-dd HH:mm:ss')) AS timestamp'
ToUnixTime(ToDate(TRIM (XPATH(x, 'record/start_ts')), 'yyyy-MM-dd HH:mm:ss')) AS start_ts'
ToUnixTime(ToDate(TRIM (XPATH(x, 'record/end_ts')), 'yyyy-MM-dd HH:mm:ss')) AS end_ts'
TRIM (XPATH(x, 'record/geo_cd')) AS geo_cd'
TRIM (XPATH(x, 'record/station_id')) AS station_id'
TRIM (XPATH(x, 'record/song_end_type')) AS song_end_type'
TRIM (XPATH(x, 'record/like')) AS like'
TRIM (XPATH(x, 'record/dislike')) AS dislike'

STORE B INTO '/user/acadgild/hadoop/project/batch${batchid}/formattedweb/'