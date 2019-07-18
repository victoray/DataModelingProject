import pandas as pd
import json
from datetime import datetime

df = pd.read_json('data/song_data/A/A/A/TRAAAAW128F429D538.json', lines=True)
song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']]

df.to_csv(path_or_buf='df.csv', index=False)
test = '{} {} test'
fix = df

print(test.format(str(df.song_id[0]), df.artist_id.to_string(index=None)))
print(song_data.to_string(index=None))

df_songs = df[['song_id', 'title', 'artist_id', 'year', 'duration']]
df_artist = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude','artist_longitude']]
print()
df = pd.read_json('data/log_data/2018/11/2018-11-01-events.json', lines=True)

# print(df.to_string())

df = df[df['page'] == 'NextSong']
t = pd.to_datetime(df['ts'])




df['ts'] = pd.to_datetime(df['ts'])
df['hour'] = df['ts'].dt.hour
df['day'] = df['ts'].dt.day
df['week'] = df['ts'].dt.week
df['month'] = df['ts'].dt.week
df['year'] = df['ts'].dt.year
df['weekday'] = df['ts'].dt.weekday_name
time_data = df[['ts', 'hour', 'day', 'week', 'month', 'year', 'weekday']]

user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]


test = ['tt'] * len(df)
df['test'] = pd.Series(test, index=df.index)

