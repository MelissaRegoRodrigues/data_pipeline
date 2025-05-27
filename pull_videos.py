import requests
import json
import polars as pl
from dotenv import load_dotenv
import os

load_dotenv()
my_key = os.getenv('YOUTUBE_API_KEY')

channel_id = 'UCa9gErQ9AE5jT2DZLjXBIdA'

url = 'https://www.googleapis.com/youtube/v3/search'

page_token = None

video_record_list = []

def getVideoRecords(response: requests.models.Response) -> list:

    video_record_list = []
    
    for raw_item in json.loads(response.text)['items']:

        if raw_item['id']['kind'] != "youtube#video":
            continue
        
        video_record = {}
        video_record['video_id'] = raw_item['id']['videoId']
        video_record['datetime'] = raw_item['snippet']['publishedAt']
        video_record['title'] = raw_item['snippet']['title']
        
        video_record_list.append(video_record)

    return video_record_list

while page_token != 0:
    params = {"key": my_key, 'channelId': channel_id, 'part': ["snippet","id"], 'order': "date", 'maxResults':50, 'pageToken': page_token}
    response = requests.get(url, params=params)

    video_record_list += getVideoRecords(response)

    try:
        page_token = json.loads(response.text)['nextPageToken']
    except:
        page_token = 0

pl.DataFrame(video_record_list).write_parquet('data/video-ids.parquet')
pl.DataFrame(video_record_list).write_csv('data/video-ids.csv')