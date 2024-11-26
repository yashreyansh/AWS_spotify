
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import boto3
  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
from pyspark.sql.functions import col, explode
from datetime import datetime
from awsglue.dynamicframe import DynamicFrame
s3_path  ="s3://spotify-etl-project-shreyansh/raw_data/to_process/"
source_df = glueContext.create_dynamic_frame.from_options(
    connection_type='s3',
    connection_options={"paths":[s3_path]},
    format="json"
)
spotify_df = source_df.toDF()
def process_album(df):
    df_album = df.withColumn("items", explode("items")).select(
        col("items.track.album.id").alias("album_id"),
        col("items.track.album.name").alias("name"),
        col("items.track.album.release_date").alias("release_date"),
        col("items.track.album.total_tracks").alias("total_tracks"),
        col("items.track.album.external_urls.spotify").alias("external_url")
        ).drop_duplicates(["album_id"])
    return df_album

def process_artist(df):
    df_artist_exploded = df.select(explode(col("items")).alias("items")).select(explode(col("items.track.artists")).alias("artist"))
    df_artist = df_artist_exploded.select(
        col("artist.id").alias("artist_id"),
        col("artist.name").alias("name"),
        col("artist.external_urls.spotify").alias("external_url")
        ).drop_duplicates(["artist_id"])
    return df_artist

def process_songs(df):
    df_song = df.select(explode(col("items")).alias("items")).select(
        col("items.track.id").alias("song_id"),
        col("items.track.name").alias("song_name"),
        col("items.track.duration_ms").alias("duration_ms"),
        col("items.track.popularity").alias("popularity"),
        col("items.track.album.id").alias("album_id")     ,
        col("items.added_at").alias("added_at"),
        #col("items.track.artists")[0].alias("artists")
        col("items.track.artists")[0]["id"].alias("artist_id")
        ).drop_duplicates(["song_id"])
    return df_song
# data reading is done
album_df = process_album(spotify_df)
artist_df = process_artist(spotify_df)
song_df = process_songs(spotify_df)
# write to S3
def write_to_s3(df, path_suffix, format_type="csv"):
    dynamic_frame = DynamicFrame.fromDF(df, glueContext, "dynamic_frame")
    
    glueContext.write_dynamic_frame.from_options(
        frame = dynamic_frame,
        connection_type="s3",
        connection_options={"path":f"s3://spotify-etl-project-shreyansh/transformed_data/{path_suffix}"},
        format=format_type
    )
write_to_s3(album_df, "album/album_transformed_{}".format(datetime.now().strftime("%Y-%m-%d_%H-%M")), "csv")
write_to_s3(artist_df, "artist/artist_transformed_{}".format(datetime.now().strftime("%Y-%m-%d_%H-%M")), "csv")
write_to_s3(song_df, "songs/songs_transformed_{}".format(datetime.now().strftime("%Y-%m-%d_%H-%M")), "csv")

def list_of_s3_objects(bucket, prefix):
    s3_client = boto3.client('s3')
    response = s3_client.list_objects_v2(Bucket = bucket, Prefix = prefix)
    keys = [content['Key'] for content in response.get('Contents',[]) if content['Key'].endswith('.json')]
    return keys
    
bucket_name = 'spotify-etl-project-shreyansh'
prefix = 'raw_data/to_process/'
spotify_keys = list_of_s3_objects(bucket_name, prefix)   # file name

def move_and_delete_files(spotify_keys, bucket):
    s3_resource = boto3.resource('s3')
    for key in spotify_keys:
        copy_source = {
            'Bucket':bucket,
            'Key':key
        }
        # define destination_key
        destination_key = 'raw_data/processed/' + key.split('/')[-1]
        
        #copy the file to new location
        s3_resource.meta.client.copy(copy_source,bucket,  destination_key)
        
        # delete the original file
        s3_resource.Object(bucket, key).delete()
        
move_and_delete_files(spotify_keys, bucket_name)


job.commit()
