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

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']

S3_PATH_LOGS = "data/log-data"
S3_PATH_SONGS = "data/song_data/*/*/*/*.json"


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = S3_PATH_SONGS
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'duration', 'year', 'artist_id').dropDuplicates(['song_id'])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.swite.save('songs_table.parquet')

    # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_name', 'artist_latitude', 'artist_location', 'artist_longitude', 'artist_id').dropDuplicates(['artist_id'])
    
    # write artists table to parquet files
    artists_table.write.save('artists_table.parquet')


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = S3_PATH_LOGS

    # read log data file
    df = spar.read.json(log_data)
    
    # filter by actions for song plays
    df = df.where(dfLog['page'] == 'NextSong')

    # extract columns for users table    
    users_table = df.select('firstName', 'lastName', 'gender', 'userId').dropDuplicates(['userId'])
    
    # write users table to parquet files
    users_table.write.save('users_table.parquet')

    # create timestamp column from original timestamp column
    # get_timestamp = udf()
    df = df.withColumn('timestamp', get_datetime_from(df.ts))
    
    # create datetime column from original timestamp column
    # get_datetime = udf()
    df = df.withColumn('timestamp', get_datetime_from(df.ts))
    
    # extract columns to create time table
    time_table = df.select('ts', 'datetime')
    
    # write time table to parquet files partitioned by year and month
    time_table.write.save('time_table.parquet')

    # read in song data to use for songplays table
    song_df = spark.read.json('data/song_data/*/*/*/*.json')

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = 

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.save('songplays_table.parquet')

@udf(TimestampType())
def get_datetime_from(long_value):
    return datetime.fromtimestamp(long_value/1000.0)

def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
