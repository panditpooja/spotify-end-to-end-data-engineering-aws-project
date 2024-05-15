#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import json
import os
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import boto3
from datetime import datetime 

def lambda_handler(event, context):
    # TODO implement
    client_id=os.environ.get('client_id')
    client_secret=os.environ.get('client_secret')
    client_credentials_manager=SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
    sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)
    top_global_playlist='https://open.spotify.com/playlist/37i9dQZEVXbNG2KDcFcKOF'
    get_playlist_id=top_global_playlist.split('/')[-1]
    data=sp.playlist_tracks(get_playlist_id)
    # print(data)
    client=boto3.client('s3')
    
    filename="spotify_raw" + str(datetime.now()) + ".json"
    client.put_object(
        Bucket="my-spotify-etl-data-pipeline-project",
        Key="raw_data/to_process/" + filename,
        Body=json.dumps(data)
        )

