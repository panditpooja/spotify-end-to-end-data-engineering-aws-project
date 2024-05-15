#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import json
import boto3
from datetime import datetime
from io import StringIO
import pandas as pd

def album(data):
    album_list=[]
    for item in data['items']:
      ID=item['track']['album']['id']
      Album_Name= item['track']['album']['name']
      Release_Date=item['track']['album']['release_date']
      Total_Tracks=item['track']['album']['total_tracks']
      External_URL=item['track']['album']['external_urls']['spotify']
      album_list.append({'ID':ID,'Album_Name':Album_Name,'Release_Date':Release_Date,'Total_Tracks':Total_Tracks, 'External_URL':External_URL})
    return album_list
    
def artist(data):
    artist_list=[]
    for item in data['items']:
      artist=item['track']['album']['artists']
      for each_artist in artist:
        artist_id=each_artist['id']
        artist_name=each_artist['name']
        external_url=each_artist['href']
        artist_list.append({'Artist_ID':artist_id,'Artist_Name':artist_name,'External_URL':external_url})
    return artist_list
    
def song(data):
    songs_list=[]
    for item in data['items']:
      song_id=item['track']['id']
      song_name=item['track']['name']
      song_duration_ms=item['track']['duration_ms']
      song_url=item['track']['external_urls']['spotify']
      song_popularity=item['track']['popularity']
      song_added=item['added_at']
      album_id=item['track']['album']['id']
      artist_id=item['track']['album']['artists'][0]['id']
      songs_list.append({'Song_ID':song_id,'Name':song_name,'Duration_ms':song_duration_ms,'URL':song_url,'Popularity':song_popularity,'Added_At':song_added,'Album_ID':album_id,'Artist_ID':artist_id})
    return songs_list  
    
def lambda_handler(event, context):
    # TODO implement
    s3=boto3.client('s3')
    Bucket="my-spotify-etl-data-pipeline-project"
    Key="raw_data/to_process/" 
    
    spotify_data=[]
    spotify_keys=[]
    for file in s3.list_objects(Bucket=Bucket,Prefix=Key)['Contents']:
        file_key= file['Key']
        if file_key.split('.')[-1]=='json':
            response=s3.get_object(Bucket=Bucket,Key=file_key)
            content=response['Body']
            jsonObject=json.loads(content.read())
            spotify_data.append(jsonObject)
            spotify_keys.append(file_key)
    
    for data in spotify_data:
        album_list = album(data)
        artist_list = artist(data)
        songs_list = song(data)
        
        # Album dataframe    
        album_df=pd.DataFrame.from_dict(album_list)
        album_df['Release_Date']=pd.to_datetime(album_df['Release_Date'])
        album_df=album_df.drop_duplicates(subset=['ID'])
        
        #Artist Dataframe
        artist_df=pd.DataFrame.from_dict(artist_list)
        artist_df=artist_df.drop_duplicates(subset=['Artist_ID'])
        
        #Song Dataframe
        song_df=pd.DataFrame.from_dict(songs_list)
        song_df['Added_At']=pd.to_datetime(song_df['Added_At'])
        
        #Store the dataframe to respective folder of transformed_data folder in S3 bucket
        album_key="transformed_data/album_data/transformed_album_data" + str(datetime.now()) + ".csv"
        album_buffer=StringIO()
        album_df.to_csv(album_buffer, index=False)
        album_content=album_buffer.getvalue()
        s3.put_object(Bucket=Bucket,Key=album_key, Body=album_content)
        
        artist_key="transformed_data/artist_data/transformed_artist_data" + str(datetime.now()) + ".csv"
        artist_buffer=StringIO()
        artist_df.to_csv(artist_buffer, index=False)
        artist_content=artist_buffer.getvalue()
        s3.put_object(Bucket=Bucket,Key=artist_key, Body=artist_content)
        
        song_key="transformed_data/song_data/transformed_song_data" + str(datetime.now()) + ".csv"
        song_buffer=StringIO()
        song_df.to_csv(song_buffer, index=False)
        song_content=song_buffer.getvalue()
        s3.put_object(Bucket=Bucket,Key=song_key, Body=song_content)
        
    #Copying the files from "to_process" folder to "processed" folder.
    s3_resource=boto3.resource('s3')
        
    for key in spotify_keys:
        copy_source={
            'Bucket':Bucket,
            'Key':key
        }
        s3_resource.meta.client.copy(copy_source,Bucket,"raw_data/processed/" + key.split('/')[-1])
        s3_resource.Object(Bucket,key).delete()

