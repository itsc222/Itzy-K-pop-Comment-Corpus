import os
from googleapiclient.discovery import build
import polars as pl
import glob
import langid

# Set your API key or OAuth credentials file path
API_KEY = 'AIzaSyCrF7Elesy9bqClDEPHWy_xy90y3B8O1f8'

youtube = build('youtube', 'v3', developerKey=API_KEY)

# Specify the video ID for which you want to retrieve comments
video_id = 'pNfTK39k55U'

schema = {
    'videoId': str,
    'title': str,
    'commentId': str,  
    'textDisplay': str,
    'authorDisplayName': str,
    'likeCount': pl.Int64,
    'publishedAt': str,
    'updatedAt': str,
}

empty_df = pl.DataFrame(schema=schema)

# Retrieve comments from the video
response = youtube.commentThreads().list(
    part='snippet, replies',
    videoId=video_id,
    textFormat='plainText',
    order = 'relevance',
    maxResults=100
).execute()

results = response['items']

video_info = youtube.videos().list(
    part='snippet,statistics',  # Specify which parts of the video data you want
    id=video_id
).execute()

title = video_info['items'][0]['snippet']['title']
print(title)

# Process and print the comments

# Generate a list of numbers from 0 to 99

numbers = list(range(len(results)))

for i in numbers:

    comments_list = response['items'][i]['snippet']

    import polars as pl

    # Define the schema with 'str' for all columns


    # Create an empty Polars DataFrame (datatable)

    # Your new input dictionary
    data = {
        'videoId': comments_list['topLevelComment']['snippet']['videoId'],
        'title': title,
        'commentId': results[i]['snippet']['topLevelComment']['id'],
        'textDisplay': comments_list['topLevelComment']['snippet']['textDisplay'],
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
print(extended_df)

# Define the schema for the Polars DataFrame without quotes
schema2 = {  
            'videoId': str,
            'title': str,
            'commentId': str,
            'textDisplay': str,
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
            print(replylen)
            replyindex = list(range(replylen))
            print(replyindex)
            replyID = replies[0]['id']
            for a in replyindex:
                replyID = replies[a]['id']

                # Create an empty Polars DataFrame with the specified schema
                empty_df = pl.DataFrame(schema=schema2)

                data2 = {  
                    'videoId': replies[a]['snippet']['videoId'],
                    'title': title,
                    'commentId': replies[a]['id'],
                    'textDisplay': replies[a]['snippet']['textDisplay'],
                    'authorDisplayName': replies[a]['snippet']['authorDisplayName'],
                    'likeCount': replies[a]['snippet']['likeCount'],
                    'publishedAt': replies[a]['snippet']['publishedAt'],
                    'updatedAt': replies[a]['snippet']['updatedAt']}
                
                df = pl.DataFrame(data2, schema=schema2)
                df_replies.extend(df)
        except KeyError:
            pass
    else:
        pass

print(df_replies)

# Specify the file path where you want to save the CSV file
csv_file_path = 'output.csv'

print(extended_df)
print(df_replies)

data_full = pl.concat([extended_df, df_replies])

videoID = comments_list['topLevelComment']['snippet']['videoId']

data_full.write_csv(f'/Users/ischneid/Itzy-K-pop-Comment-Corpus/dataframes/{title[0:20]}.csv')

# Step 2: Use glob to retrieve file paths
folder_path = '/Users/ischneid/Itzy-K-pop-Comment-Corpus/dataframes'  # Replace with the path to your folder
file_pattern = '*.csv'  # Adjust the pattern to match your file type (e.g., '*.csv', '*.parquet')

file_paths = glob.glob(f'{folder_path}/{file_pattern}')

# Initialize an empty list to store Polars DataFrames
dfs = []

# Step 3: Loop through file paths and read Polars DataFrames
for file_path in file_paths:
    df = pl.read_csv(file_path)  # Use pl.read_parquet() for Parquet files, adjust as needed
    dfs.append(df)

# Step 4: Combine Polars DataFrames into a single DataFrame
combined_df = pl.concat(dfs)

comments = combined_df['textDisplay'].to_list()

df_lang_schema = {'textDisplay': str,
                  'language': str,
                  'confidence':pl.Float64}

df_lang = pl.DataFrame(schema=df_lang_schema)

for phrase in comments:
    languages = ['en', 'zh', 'pt', 'ru', 'es', 'ko']
    langid.set_languages(languages)
    lang, confidence = langid.classify(phrase)

    data = {'textDisplay': phrase,
            'language': lang,
            'confidence': confidence}

    df_phrase = pl.DataFrame(data)

    df_lang.extend(df_phrase)

print(df_lang)

combined_df = combined_df.join(df_lang, on = 'textDisplay', how = 'inner')


combined_df.write_csv('/Users/ischneid/Itzy-K-pop-Comment-Corpus/full_table.csv')

# Now, 'combined_df' contains all the data from the files in the folder.

print(df_lang)