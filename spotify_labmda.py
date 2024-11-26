import json
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import os
import boto3
from datetime import datetime

def lambda_handler(event, context):
    client_id = os.environ.get('SPOTIPY_CLIENT_ID')
    client_secret = os.environ.get('SPOTIPY_CLIENT_SECRET')

    client_credential_manager = SpotifyClientCredentials(client_id= client_id,client_secret=client_secret)
    sp = spotipy.Spotify(auth_manager=client_credential_manager)
    playlist_link = "https://open.spotify.com/playlist/37i9dQZEVXbNG2KDcFcKOF"
    playlist_URI = playlist_link.split('/')[-1]
    
    data = sp.playlist_tracks(playlist_URI)

    client = boto3.client('s3')

    filename = "spotify_raw_" + str(datetime.now()) + ".json"

    client.put_object(
        Bucket="spotify-etl-project-shreyansh",
        Key = "raw_data/to_process/" +filename,
        Body=json.dumps(data)
    )

    glue = boto3.client("glue")
    gluejobname = "spotify_transformation_job_1"

    try :
        runId = glue.start_job_run(JobName =gluejobname)
        status = glue.get_job_run(JobName =gluejobname , RunId=runId['JobRunId'])
        print("Job Status: ", status['JobRun']['JobRunState'])
    except Exception as e:
        print(e,"hola shreyansh")
