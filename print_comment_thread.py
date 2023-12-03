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


def print_comment(id):

    request = youtube.commentThreads().list(
            part="snippet",
            id=id,
            order = "relevance",
            maxResults = 100
        )
    response = request.execute()
    response = response['items'][0]

    # print(response)

    comment_id = response['id']
    authorDisplayName = response['snippet']['topLevelComment']['snippet']['authorDisplayName']
    textDisplay = response['snippet']['topLevelComment']['snippet']['textDisplay']
    textDisplay_mod = textDisplay.replace('<br>', '\n')
    likeCount = response['snippet']['topLevelComment']['snippet']['likeCount']
    publishedAt = response['snippet']['topLevelComment']['snippet']['publishedAt']

    # Function to write objects to a text file
    def write_comment_to_file(file_path):
        with open(file_path, 'a') as file:
            file.write('\n' + 
                        "@" + str(authorDisplayName) + '  ' + str(publishedAt) + ':' + '\n' + 
                        str(textDisplay_mod) + "\n" +
                        str(likeCount)  + "likes" + '\n\n')

    # Example usage
    file_path = f'/Users/ischneid/Itzy-K-pop-Comment-Corpus/Video_Comment_Threads/{song_title}.txt'
    write_comment_to_file(file_path)

    def write_replies_to_file(file_path):
        with open(file_path, 'a') as file:
            file.write('\n' + 
                    '       ' +
                    "@" + str(authorDisplayName) + ' '  + str(publishedAt) + ':' + '\n'
                    '       ' +
                    str(textDisplay_mod) + "\n" +
                    '       ' + 
                    str(likeCount)  + "likes" +
                    '\n\n')

    request = youtube.comments().list(
            part="snippet",
            maxResults = 100,
            parentId=id
        )
    response = request.execute()['items']
    # print(response)

    index = (len(response))
    index = list(range(index))

    for i in index:
        authorDisplayName = response[i]['snippet']['authorDisplayName']
        textDisplay = response[i]['snippet']['textDisplay']
        textDisplay_mod = textDisplay.replace('<br>', '\n       ')
        likeCount = response[i]['snippet']['likeCount']
        publishedAt = response[i]['snippet']['publishedAt']
        write_replies_to_file(file_path)


def scrape_comments_id(video_id):
        response = youtube.commentThreads().list(
        part='snippet, replies',
        videoId=video_id,
        textFormat='plainText',
        order = 'relevance',
        maxResults=100
    ).execute()
        
        results = response['items']
        index = len(results)
        index = list(range(index))

        id_list = []
        for i in index:
            id = results[i]['id']
            id_list.append(id)
        return(id_list)

def write_txt_file(song_title):
    with open(file_path, 'w') as file:
        file.write(str(song_title) + '\n\n')

########Enter Video ID Here###########

video_id = 'ZoIlubL-TOM'

########Enter Video ID Here###########


request = youtube.videos().list(
        part="snippet",
        id=video_id
    )

response = request.execute()
song_title = response['items'][0]['snippet']['title'[:20]]
file_path = f'/Users/ischneid/Itzy-K-pop-Comment-Corpus/Video_Comment_Threads/{song_title}.txt'


id_list = scrape_comments_id(video_id)

write_txt_file(song_title)
for id in id_list:
    print_comment(id)

