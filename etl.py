import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
import databricks.koalas as ks
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
    """create spark session"""
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """ process song_data to ctreate songs, artist tables """
    # get filepath to song data file
    song_data = 'data/song_data/A/B/C/*.json'
    kdf = ks.read_json('data/song_data/A/B/C/*.json')
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    #song_id, title, artist_id, year, duration
    songs_table = (ks.sql("""select
                                DISTINCt
                                row_number() over (ORDER BY year,title,artist_id) id,
                                song_id,
                                title,
                                artist_id,
                                year,
                                duration
                                FROM {kdf}
        """))
    
    # write songs table to parquet files partitioned by year and artist
    (songs_table
     .to_spark()
     .write
     .partitionBy("year","artist_id")
     .parquet(f'{output_data}/songs',mode="overwrite"))

    # extract columns to create artists table
    artists_table =  (ks.sql(""" SELECT
                            DISTINCT
                            row_number() over (ORDER BY artist_name) id,
                            artist_id,
                            artist_name,
                            artist_location,
                            artist_latitude,
                            artist_longitude
                            FROM {kdf}
                            
                            
"""))
    
    # write artists table to parquet files
    (artists_table
     .to_spark()
     .write
     .parquet(f'{output_data}/artists',mode="overwrite"))


def process_log_data(spark, input_data, output_data):
    """process log_data to create users, time ,songsplay table"""
    # get filepath to log data file
    log_data ='data/*.json'
    

    # read log data file
    log_kdf = ks.read_json(log_data)

    # filter by actions for song plays
    df = log_kdf.filter(log_kdf.page == "NextSong") 

    # extract columns for users table    
    users_table = ks.sql(""" SELECT 
                           DISTINCT
                           userId,
                           firstName,
                           lastName,
                           gender,
                           level 
                           FROM {df}""")
    
    # write users table to parquet files
    (users_table
     .to_spark()
     .write
     .parquet(f'{output_data}/users',mode="overwrite"))

    # create timestamp column from original timestamp column
    df['timestamp']=ks.to_datetime(df['ts'],unit='ns') 
    
    # create datetime column from original timestamp column
    df['datetime']=ks.to_datetime(df['ts'])
    
    # extract columns to create time table
    time_table =(ks.sql("""
            SELECT
            DISTINCT
           datetime as start_time,
           extract(day from datetime) as day,
           extract(week from datetime) as week,
           extract(month from datetime) as month,
           extract(year from datetime) as year,
           extract(hour from datetime) as hour
           from {df}
                        """
    )) 
    
    # to enable join on table
    ks.set_option('compute.ops_on_diff_frames', True)
    
    # add weekday columns
    time_table['weekday']=df.datetime.dt.weekday
    
    # write time table to parquet files partitioned by year and month
    (time_table
    .to_spark()
    .write
    .partitionBy('year','month')
    .parquet('time/'))

    # read in song data to use for songplays table
    song_df = ks.read_json('data/song_data/*/*/*/*.json')

    # convert ts to datetime 
    log_kdf["ts"]= ks.to_datetime(log_kdf['ts'])
    
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = (ks.sql(""" SELECT 
                             DISTINCT
                             row_number() over (ORDER BY e.userId) songplay_id,
                             e.ts AS start_time,
                             extract(month from e.ts) as month,
                             extract(year from e.ts) as year,
                             e.userId AS user_id,
                             e.level AS level,
                             s.song_id AS song_id,
                             s.artist_id AS artist_id,
                             e.sessionId as session_id,
                             e.location AS location,
                             e.userAgent AS user_agent
                             FROM {log_kdf} as e join {song_df} as s ON
                             (e.artist = s.artist_name AND 
                             e.song = s.title AND 
                             e.length= s.duration)
                             WHERE e.page='NextSong'

             """))

    # write songplays table to parquet files partitioned by year and month
    (songplays_table
    .to_spark()
    .write
    .partitionBy("year","month") 
    .parquet(f'{output_data}/songplayes',mode="overwrite")
    )


def main():
        spark = create_spark_session()
        input_data = "s3a://udacity-dend/"
        output_data = ""

        process_song_data(spark, input_data, output_data)    
        process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
