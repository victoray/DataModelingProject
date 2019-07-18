import os
import glob
import psycopg2
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session, sessionmaker

from sql_queries import *
import json
from create_tables import dbname, dbuser, dbpass, host, port

engine = create_engine(f'postgresql://{dbpass}:{dbuser}@{host}:{port}/sparkifydb', echo=True)
session = sessionmaker(bind=engine)()
session.connection().connection.set_isolation_level(0)


def process_song_file(filepath):
    # open song file
    df = pd.read_json(filepath, lines=True)

    try:
        # insert song record
        song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']]
        # cur.execute(song_table_insert, song_data)
        song_data.to_sql('songs', con=engine, if_exists='append', index=False)
    except IntegrityError as e:
        print('Integrity error: {}'.format(e.args[0]))

    try:
        # insert artist record
        artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']]
        artist_data.columns = ['artist_id', 'name', 'location', 'latitude', 'longitude']
        artist_data.to_sql('artists', con=engine, if_exists='append', index=False)
    except IntegrityError as e:
        print('Integrity error: {}'.format(e.args[0]))


def process_log_file(filepath):
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    df['start_time'] = pd.to_datetime(df['ts'])
    df['hour'] = df['start_time'].dt.hour
    df['day'] = df['start_time'].dt.day
    df['week'] = df['start_time'].dt.week
    df['month'] = df['start_time'].dt.month
    df['year'] = df['start_time'].dt.year
    df['weekday'] = df['start_time'].dt.weekday_name

    try:
        # insert time data records
        time_df = df[['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday']]

        # for i, row in time_df.iterrows():
        #     cur.execute(time_table_insert, list(row))

        time_df.to_sql('time', con=engine, if_exists='append', index=False)
    except IntegrityError as e:
        print('Integrity error: {}'.format(e.args[0]))

    # try:
    #     # load user table
    #     user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]
    #     user_df.columns = ['user_id', 'first_name', 'last_name', 'gender', 'level']
    #     print(user_df.to_string())
    #     user_df.to_sql('users', con=conn, if_exists='append', index=False, chunksize=1)
    # except IntegrityError as e:
    #     print('Integrity error: {}'.format(e.args[0]))

    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]
    user_df.columns = ['user_id', 'first_name', 'last_name', 'gender', 'level']
    # insert user records
    for i, row in user_df.iterrows():
        try:
            session.execute(user_table_insert % (row.user_id, row.first_name, row.last_name, row.gender, row.level))
            session.commit()
        except IntegrityError as e:
            print('Integrity error: {}'.format(e.args[0]))

    # insert songplay records
    for index, row in df.iterrows():

        # get songid and artistid from song and artist tables
        results = session.execute(song_select % (row.song, row.artist, row.length))
        results = results.fetchall()

        if results:
            songid, artistid = results[0]
            print('results', results)
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (row.start_time, row.userId, row.level,
                         songid, artistid,
                         row.sessionId,
                         row.location, row.userAgent)

        session.execute(songplay_table_insert % songplay_data)


def process_data(filepath, func):
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(datafile)
        print('{}/{} files processed.'.format(i, num_files))


def main():
    print(engine.table_names())

    process_data(filepath='data/song_data', func=process_song_file)
    process_data(filepath='data/log_data', func=process_log_file)


if __name__ == "__main__":
    main()
