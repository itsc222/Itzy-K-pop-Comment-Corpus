import os
from googleapiclient.discovery import build
import polars as pl
import glob
import langid

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

# Specify the video ID for which you want to retrieve comments
video_ids = ['-8Z6XQAaLq8'
             ]


schema = {
    'videoId': str,
    'title': str,
    'parentId': str,  
    'textDisplay': str,
    'authorChannelId': str,
    'authorDisplayName': str,
    'likeCount': pl.Int64,
    'publishedAt': str,
    'updatedAt': str,
}

empty_df = pl.DataFrame(schema=schema)

for video_id in video_ids:

    # Retrieve comments from the video
    response = youtube.commentThreads().list(
        part='snippet, replies',
        videoId=video_id,
        textFormat='plainText',
        order = 'relevance',
        maxResults=100
    ).execute()

    results = response['items']

    # print(results)
    # quit()

    video_info = youtube.videos().list(
        part='snippet,statistics',  # Specify which parts of the video data you want
        id=video_id
    ).execute()

    title = video_info['items'][0]['snippet']['title']
    # print(title)

    # Process and print the comments

    # Generate a list of numbers from 0 to 99

    numbers = list(range(len(results)))

    for i in numbers:

        comments_list = response['items'][i]['snippet']

        import polars as pl

        authorChannelId = str(comments_list['topLevelComment']['snippet']['authorChannelId']['value'])
        authorChannelId = authorChannelId.replace('{', '').replace('}', '').replace('"', '')

        # print((authorChannelId))
        # quit()

        # Create an empty Polars DataFrame (datatable)

        # Your new input dictionary
        data = {
            'videoId': comments_list['topLevelComment']['snippet']['videoId'],
            'title': title,
            'parentId': results[i]['snippet']['topLevelComment']['id'],
            'textDisplay': comments_list['topLevelComment']['snippet']['textDisplay'],
            'authorChannelId': authorChannelId,
            'authorDisplayName': comments_list['topLevelComment']['snippet']['authorDisplayName'],
            'likeCount': comments_list['topLevelComment']['snippet']['likeCount'],
            'publishedAt': comments_list['topLevelComment']['snippet']['publishedAt'],
            'updatedAt': comments_list['topLevelComment']['snippet']['updatedAt']
        }

        df = pl.DataFrame(data, schema=schema)

        #print(df)
        # Extend the empty dataframe with the data from the new dictionary
        extended_df = empty_df.extend(df)

    # Print the extended datatable
    # print(extended_df)

    # Define the schema for the Polars DataFrame without quotes
    schema2 = {  
                'videoId': str,
                'title': str,
                'parentId': str,
                'textDisplay': str,
                'authorChannelId': str,
                'authorDisplayName': str,
                'likeCount': int,
                'publishedAt': str,
                'updatedAt': str}

    df_replies = pl.DataFrame(schema=schema2)


    for i in numbers:
        if results[i]['snippet']['totalReplyCount'] > 0:
            try:
                replies = results[i]['replies']['comments']
                replylen = len(replies)
                # print(replylen)
                replyindex = list(range(replylen))
                # print(replyindex)
                replyID = replies[0]['id']
                for a in replyindex:
                    replyID = replies[a]['id']

                    # Create an empty Polars DataFrame with the specified schema
                    empty_df = pl.DataFrame(schema=schema2)

                    authorChannelId = str( replies[a]['snippet']['authorChannelId'])
                    authorChannelId = authorChannelId.replace('{', '').replace('}', '').replace('"', '')


                    data2 = {  
                        'videoId': replies[a]['snippet']['videoId'],
                        'title': title,
                        'parentId': replies[a]['id'],
                        'textDisplay': replies[a]['snippet']['textDisplay'],
                        'authorChannelId': authorChannelId,
                        'authorDisplayName': comments_list['topLevelComment']['snippet']['authorDisplayName'],
                        'likeCount': replies[a]['snippet']['likeCount'],
                        'publishedAt': replies[a]['snippet']['publishedAt'],
                        'updatedAt': replies[a]['snippet']['updatedAt']}
                    
                    df = pl.DataFrame(data2, schema=schema2)
                    df_replies.extend(df)
            except KeyError:
                pass
        else:
            pass

    # print(df_replies)

    # Specify the file path where you want to save the CSV file
    csv_file_path = 'output.csv'

    # print(extended_df)
    # print(df_replies)

    data_full = pl.concat([extended_df, df_replies])

    videoID = comments_list['topLevelComment']['snippet']['videoId']

    data_full.write_csv(f'/Users/ischneid/Itzy-K-pop-Comment-Corpus/dataframes/{video_id}.csv')


print(data_full)

data_full.write_csv(f'/Users/ischneid/Itzy-K-pop-Comment-Corpus/{title}.csv')