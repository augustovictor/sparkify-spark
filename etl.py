import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

S3_PATH_LOGS = "log_data/*/*/*.json"
# S3_PATH_SONGS = "song_data/*/*/*/*.json"
S3_PATH_SONGS = "song_data/A/A/A/*.json"
S3_OUTPUT_PATH ="s3a://victor-nano-sparkify-raw-data/sparkify-parquet"


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()

    spark.conf.set("mapreduce.fileoutputcommitter.algorithm.version", "2")

    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    songs_path = f"{input_data}/{S3_PATH_SONGS}"
    
    # read song data file
    df = spark.read.json(songs_path)

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'duration', 'year', 'artist_id', 'artist_name').dropDuplicates(['song_id'])
    
    # partition songs_table by year and then artist
    songs_table = songs_table.repartition('year', 'artist_name')

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_name').mode('overwrite').save(f"{S3_OUTPUT_PATH}/songs_table.parquet")

    # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_name', 'artist_latitude', 'artist_location', 'artist_longitude').dropDuplicates(['artist_id'])
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').save(f"{S3_OUTPUT_PATH}/artists_table.parquet")


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    logs_path = f"{input_data}/{S3_PATH_LOGS}"

    # read log data file
    logs_df = spark.read.json(logs_path)
    
    # filter by actions for song plays
    logs_df = logs_df.where(logs_df['page'] == 'NextSong')

    # extract columns for users table    
    users_table = logs_df\
        .select('firstName', 'lastName', 'gender', 'userId')\
        .dropDuplicates(['userId'])
    
    # write users table to parquet files
    users_table.write.mode('overwrite').save(f"{S3_OUTPUT_PATH}/users_table.parquet")

    # create timestamp column from original timestamp column
    # get_timestamp = udf()
    logs_df = logs_df.withColumn('timestamp', get_datetime_from(logs_df.ts))
    
    # create datetime column from original timestamp column
    # get_datetime = udf()
    logs_df = logs_df.withColumn('datetime', get_datetime_from(logs_df.ts))
    
    # extract columns to create time table
    # break fields into: start_time, hour, day, week, month, year, weekday
    time_table = logs_df.select(logs_df.datetime.alias('start_time'))
    time_table = time_table.withColumn('as', F.year('start_time'))\
    .withColumn('day', F.dayofmonth('start_time'))\
    .withColumn('week', F.weekofyear('start_time'))\
    .withColumn('month', F.month('start_time'))\
    .withColumn('year', F.year('start_time'))\
    .withColumn('weekday', F.dayofweek('start_time'))

    # partition time_table by year and month
    time_table = time_table.repartition('year', 'month')
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').mode('overwrite').save(f"{S3_OUTPUT_PATH}/time_table.parquet")

    # read in song data to use for songplays table
    song_path = f"{input_data}/{S3_PATH_SONGS}"
    song_df = spark.read.json(song_path)

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = song_df.join(
        logs_df, 
        (song_df.duration == logs_df.length)
        & (song_df.artist_name == logs_df.artist)
        & (song_df.title == logs_df.song)
    ).select(logs_df.ts, logs_df.userId.alias('user_id'), logs_df.level, song_df.song_id, song_df.artist_id, logs_df.sessionId.alias('session_id'), logs_df.location, logs_df.userAgent.alias('user_agent'))\
    .withColumn('songplay_id', F.monotonically_increasing_id())\
    .withColumn('start_time', get_datetime_from(logs_df.ts))\
    .withColumn('year', F.year('start_time'))\
    .withColumn('month', F.month('start_time'))

    # partition songplays_table by year and month
    songplays_table = songplays_table.repartition('year', 'month')

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month').mode('overwrite').save(f"{S3_OUTPUT_PATH}/songplays_table.parquet")

@udf(TimestampType())
def get_datetime_from(long_value):
    return datetime.fromtimestamp(long_value/1000.0)

def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend"
    output_data = S3_OUTPUT_PATH
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
