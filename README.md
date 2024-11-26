# AWS_spotify

1. spotify_labmda : this can be schedules with event to run at any scheduled time.
   a. Purpose: to fetch json data about the top 50 songs from spotify api and store in folder in bucket and initiating the Glue job for transformation.

2. spotify_transformation_glue_job : this job does multiple things
   ( a.)  takes raw json data from raw/to_process folder and transforms it in songs/album/artist dataframe by fetch various values from the raw json.
   ( b.) stores those dataframes as a csv file in tranformed_data/{type} folder. From which Snowflake can fetch the files and store data as soon as the event         
      notification is triggered for the s3 bucket folder. (event has to be added for s3 bucket)
   ( c.) then the raw file in raw/to_process folder is pointless as its already processed as per point 2.b hence it will list out the file in /raw/to_process folder and move(copy & delete) the files to /raw/processed folder.
   
