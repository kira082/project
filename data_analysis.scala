import org.apache.spark.{SparkConf,SparkContext}
import org.apache.spark.sql

object DataAnalysis
{

def main(args:Array[String]) : Unit = {

val conf =  new SparkConf().setAppName ("Data Formetting")
val sc = new SparkContext(conf)
val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
val batchId = args(0)

val create_tables  = """ CREATE TABLE IF NOT EXISTS top_10_stations
						(
							station_id STRING,
							total_distinch_songs_played INT,
							distinct_user_count INT
						)
						 PARTITIONED BY 
						 (
							batchid INT
						 )
						 ROW FORMAT DELIMITED BY ','
						 STORED AS TEXTFILE
						 """

}

val load_data = s""" INSERT OVERWRITE TABLE top_10_stations
					 PARTITION (batchid :$batchid)
					 SELECT 
					 station_id,
					 COUNT(DISTINCT song_id) AS total_distinct_songs_played,
					 COUNT(DISTINCT song_id) AS distinct_user_count
					 FROM enriched_data
					 WHERE status= 'pass'
					 AND batchid = $batchid
					 AND like =1
					 GROUP BY station_id ORDER BY total_distinct_songs_played DESC
					 LIMIT 10
					 """
	try 
	{
		
		sqlContext.sql ("SET hive.auto.convert.join = false")
		sqlContext.sql("SET hive.exce.dynamic.partition.mode = nonstrict")
		sqlContext.sql("Use project")
		sqlContext.sql(create_hive_tables)
		sqlContext.sql(load_data)
		}
		catch{
		case e : Exception=>e.printStackTrace()
		}
		
		
val create_tables1  = """ CREATE TABLE IF NOT EXISTS user_behav
						(
							user_type STRING,
							duration INT
						)
						 PARTITIONED BY 
						 (
							batchid INT
						 )
						 ROW FORMAT DELIMITED BY ','
						 STORED AS TEXTFILE
						 """

}

val load_data1 = s""" INSERT OVERWRITE TABLE user_behav
					 PARTITION (batchid :$batchid)
					 SELECT 
					 CASE WHEN(su.user_id IS NULL OR CAST(ed.timestamp AS DECIMAL(20,0)) > CAST(su.subscn_end_dt AS DECIMAL(20,0))) THEN 'UNSUBSCRIBED'
					 CASE WHEN(su.user_id IS NULL OR CAST(ed.timestamp AS DECIMAL(20,0)) <= CAST(su.subscn_end_dt AS DECIMAL(20,0))) THEN 'SUBSCRIBED'
					 END AS user_type,
					 SUM(ABS(CAST(ed.end_ts AS DECIMAL(20,0)).CAST(ed.start_ts AS DECIMAL(20,0)))) AS duration
					 FROM enriched_data ed
					 LEFT OUTER JOIN subscribed_user su
					 ON ed.user_id = su.user_id
					 WHERE ed.status = 'pass'
					 AND ed.batch = $batch
					 GROUP BY CASE WHEN(su.user_id IS NULL OR CAST(ed.timestamp AS DECIMAL(20,0)) > CAST(su.subscn_end_dt AS DECIMAL(20,0))) THEN 'UNSUBSCRIBED'
					 CASE WHEN(su.user_id IS NULL OR CAST(ed.timestamp AS DECIMAL(20,0)) <= CAST(su.subscn_end_dt AS DECIMAL(20,0))) THEN 'SUBSCRIBED'
					 END
					 """
	try 
	{
		
		sqlContext.sql ("SET hive.auto.convert.join = false")
		sqlContext.sql("SET hive.exce.dynamic.partition.mode = nonstrict")
		sqlContext.sql("Use project")
		sqlContext.sql(create_hive_tables1)
		sqlContext.sql(load_data1)
		}
		catch{
		case e : Exception=>e.printStackTrace()
		}
		
val create_tables2  = """ CREATE TABLE IF NOT EXISTS connected_artists
						(
							artist_id STRING,
							user_count INT
						)
						 PARTITIONED BY 
						 (
							batchid INT
						 )
						 ROW FORMAT DELIMITED BY ','
						 STORED AS TEXTFILE
						 """

}

val load_data2 = s""" INSERT OVERWRITE TABLE connected_artists
					 PARTITION (batchid :$batchid)
					 SELECT 
					 ua.artist_id,
					 COUNT(DISTINCT ua.user_id) AS user_count
					 FROM 
					 (
					 SELECT user_id,artist_id FROM users_artists
					 LATERAL VIEW explode(artists_array) artists AS artist_id
					 ) ua
					 INNER JOIN
					 (
					 SELECT artist_id,song_id,user_id
					 FROM enriched_data
					 WHERE status = 'pass'
					 AND batchid = $batchid
					 ) ed
					 ON ua.user_id = ed.artist_id
					 AND ua.user_id = ed.user_id
					 GROUP BY ua.artist_id
					 ORDER BY user_count DESC
					 LIMIT 10
					 """
	try 
	{
		
		sqlContext.sql ("SET hive.auto.convert.join = false")
		sqlContext.sql("SET hive.exce.dynamic.partition.mode = nonstrict")
		sqlContext.sql("Use project")
		sqlContext.sql(create_hive_tables2)
		sqlContext.sql(load_data2)
		}
		catch{
		case e : Exception=>e.printStackTrace()
		}
		
		
val create_tables3  = """ CREATE TABLE IF NOT EXISTS top_10_royalty_songs						(
							song_id STRING,
							duration INT
						)
						 PARTITIONED BY 
						 (
							batchid INT
						 )
						 ROW FORMAT DELIMITED BY ','
						 STORED AS TEXTFILE
						 """

}

val load_data3 = s""" INSERT OVERWRITE TABLE top_10_royalty_songs
					 PARTITION (batchid :$batchid)
					 SELECT 
					 song_id,
					 SUM(ABS(CAST(end_ts AS DECIMAL(20,0)) - CAST( start_ts AS DECIMAL(20,0)))) AS duration
					 FROM enriched_data
					 WHERE status = 'pass'
					 AND batchid = $batchid
					 AND (like = 1 OR song_end_type = 0)
					 GROUP BY song_id
					 ORDER BY duration DESC
					 LIMIT 10
					 """
	try 
	{
		
		sqlContext.sql ("SET hive.auto.convert.join = false")
		sqlContext.sql("SET hive.exce.dynamic.partition.mode = nonstrict")
		sqlContext.sql("Use project")
		sqlContext.sql(create_hive_tables3)
		sqlContext.sql(load_data3)
		}
		catch{
		case e : Exception=>e.printStackTrace()
		}
		
val create_tables4  = """ CREATE TABLE IF NOT EXISTS top_10_unsubscribed_users						(
							user_idd STRING,
							duration INT
						)
						 PARTITIONED BY 
						 (
							batchid INT
						 )
						 ROW FORMAT DELIMITED BY ','
						 STORED AS TEXTFILE
						 """

}

val load_data4 = s""" INSERT OVERWRITE TABLE top_10_unsubscribed_users
					 PARTITION (batchid :$batchid)
					 SELECT 
					 ed.user_id,,
					 SUM(ABS(CAST(ed.end_ts AS DECIMAL(20,0)) - CAST( ed.start_ts AS DECIMAL(20,0)))) AS duration
					 FROM enriched_data ed
					 LEFT OUTER JOIN subscribed_users su
					 ON ed.user_id = su.user_id
					 WHERE ed.status = 'pass'
					 AND ed.batchid = $batchid
					 AND (su.user_id IS NULL OR (CAST(ed.timstamp AS DECIMAL(20,0))))
					 GROUP BY ed.user_id
					 ORDER BY duration DESC
					 LIMIT 10
					 """
	try 
	{
		
		sqlContext.sql ("SET hive.auto.convert.join = false")
		sqlContext.sql("SET hive.exce.dynamic.partition.mode = nonstrict")
		sqlContext.sql("Use project")
		sqlContext.sql(create_hive_tables4)
		sqlContext.sql(load_data4)
		}
		catch{
		case e : Exception=>e.printStackTrace()
		}		
		
		
}
}