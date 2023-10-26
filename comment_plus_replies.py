import os
from googleapiclient.discovery import build
import polars as pl
import glob
import langid
import requests

# Set your API key or OAuth credentials file path
file_path = '/Users/ischneid/Itzy-K-pop-Comment-Corpus/apikey.txt'
try:
    with open(file_path, 'r') as file:
        API_KEY = file.read()
except FileNotFoundError:
    print(f"File '{file_path}' not found.")
except Exception as e:
    print(f"An error occurred: {e}")
API_KEY = 'AIzaSyCrF7Elesy9bqClDEPHWy_xy90y3B8O1f8'

youtube = build('youtube', 'v3', developerKey=API_KEY)

parentId = "UgzASpQyT3mx6LJbdWB4AaABAg"

request = youtube.comments().list(
        part="snippet",
        maxResults = 100,
        parentId=parentId
    )
response = request.execute()

# print(response)

full_table = pl.read_csv('/Users/ischneid/Itzy-K-pop-Comment-Corpus/full_table.csv')

og_comment = full_table.filter(full_table["parentId"] == parentId)
og_comment = og_comment.select('title', 
                               'textDisplay', 
                               'parentId', 
                               'authorChannelId',
                               'authorDisplayName', 
                               'likeCount', 
                               'publishedAt')

title = og_comment[1]

comment_count = (len(response['items']))

index = list(range(comment_count))

schema = {
    'title': str,
    'textDisplay': str,
    'parentId': str,
    'authorChannelId': str,
    'authorDisplayName': str,
    'likeCount': pl.Int64,
    'publishedAt': str}

df_full = pl.DataFrame(schema = schema)
# print(df_full)

for i in index:

    
    title = og_comment[0, 0]
    id = (response['items'][i]['id'])

    authorChannelId = response['items'][i]['snippet']['authorChannelId']['value']
    authorChannelId = str(authorChannelId)
    authorChannelId = authorChannelId.replace('{', '').replace('}', '').replace('"', '')

    textDisplay = response['items'][i]['snippet']['textDisplay']
    parentId = response['items'][i]['snippet']['parentId']
    authorDisplayName = response['items'][i]['snippet']['authorDisplayName']
    likeCount = response['items'][i]['snippet']['likeCount']
    publishedAt = response['items'][i]['snippet']['publishedAt']

    data = {
    'title': title,
    'textDisplay': textDisplay,
    'parentId': parentId,
    'authorChannelId': authorChannelId,
    'authorDisplayName': authorDisplayName,
    'likeCount': likeCount,
    'publishedAt': publishedAt}

    # print(data)

    # print(data)

    df = pl.DataFrame(data, schema = schema)

    df_full.extend(df)

# print(df_full)

full_comment_thread = og_comment.extend(df_full)

author_ids = (full_comment_thread[ : , 3]).to_list()

# print(author_ids)

country_df_schema = {'authorChannelId' : str,
                     'country': str}

country_df = pl.DataFrame(schema = country_df_schema)

# for id in author_ids:
#     request2 = youtube.channels().update(
#         part=("brandingSettings"),
#         id= id)
#     response2 = request2.execute()
#     print(response2)
#     quit()

full_comment_thread.write_csv(f'/Users/ischneid/Itzy-K-pop-Comment-Corpus/comment_threads/{parentId}.csv')

