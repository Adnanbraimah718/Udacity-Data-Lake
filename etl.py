import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, to_timestamp
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.functions import from_unixtime, monotonically_increasing_id


config = configparser.ConfigParser()
config.read('cred.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS CREDS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS CREDS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = 's3a://udacity-dend/song_data/*/*/*/*.json'
    
    # read song data file
    df_songs = spark.read.csv(song_data)

    # extract columns to create songs table
    songs_table = df_songs.select("song_id", "title", "artist_id", "year", "duration")
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy("year", "artist_id").parquet(output_data + "songs")

    # extract columns to create artists table
    artists_table = df_songs.select("artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude")
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data + "artists")


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = 's3a://udacity-dend/log_data/*/*/*/*.json'

    # read log data file
    df_log = spark.read.json(log_data)
    
    # filter by actions for song plays
    df_log = df_log.filter(df_log['page'] == 'NextSong')

    # extract columns for users table    
    users_table = df_log.select("userId", "firstName", "lastName", "gender", "level")
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(output_data + "users")

    # create timestamp column from original timestamp column
    get_timestamp = F.udf(lambda x: datetime.fromtimestamp( (x/1000.0) ), T.TimestampType()) 
    df_log = df_log.withColumn("timestamp", get_timestamp(df_log.ts))
    
    # create datetime column from original timestamp column
    df_log = df_log.withColumn('date_time', from_unixtime(df_log.ts/1000).cast(dataType=T.TimestampType()))
    
    # extract columns to create time table
    #start_time, hour, day, week, month, year, weekday
    time_table = df_log.select(col("date_time").alias("start_time"),
                           year(col('date_time')).alias('year'),
                           month(col('date_time')).alias('month'),
                           dayofmonth(col('date_time')).alias('day'),
                           hour(col('date_time')).alias('hour'),
                           weekofyear(col('date_time')).alias('week')
                          )
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year","month").parquet(output_data+"time")
 

    # extract columns from joined song and log datasets to create songplays table 
    
    #must change dataframe to table for SQL functions
    df_log.createOrReplaceTempView("log_df_table")
    df_songs.createOrReplaceTempView("song_df_table")
    time_table.createOrReplaceTempView("time_table_table")
    
    songplays_table = spark.sql("""
                                SELECT DISTINCT log_df_table.userId, log_df_table.level, log_df_table.location,
                                log_df_table.userAgent, log_df_table.sessionId, log_df_table.date_time, 
                                song_df_table.artist_id, song_df_table.song_id, 
                                time_table_table.month, time_table_table.year 
                                FROM log_df_table
                                JOIN song_df_table
                                ON song_df_table.artist_name = log_df_table.artist
                                JOIN time_table_table
                                ON time_table_table.start_time = log_df_table.date_time
                                """
                                )
    #monotonically_increasing_id assigns unique id number to each row
    songplays_table = songplays_table.withColumn("songplay_id", monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year","month").parquet(output_data+"songplays")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    ##Create S3 bucket and enter here
    output_data = "s3a://'Enter bucket name'/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
