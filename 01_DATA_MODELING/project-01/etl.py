import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    
    """
    process song files from json data. it requires 2 paramaters
    cur: cursor for database connections
    filepath: os path to the file to process
    """
    
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[['song_id','title','artist_id','year','duration']]
    
    song_data = song_data.values[0].tolist()
    
    #print(song_data)
    
    try:
        cur.execute(song_table_insert, song_data)
    except psycopg2.Error as e: 
        print("Error: inserting song data into the Database")
        print(e)
    
    # insert artist record
    artist_data = df[['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']]
    
    artist_data = artist_data.values[0].tolist()
    
    try: 
        #print (artist_data)
        cur.execute(artist_table_insert, artist_data)
    except psycopg2.Error as e: 
        print("Error: inserting artist data into the Database")
        print(e)

def process_log_file(cur, filepath):
    
    """
    process log files from json data. it requires 2 paramaters
    cur: cursor for database connections
    filepath: os path to the file to process
    """
    
    # list to store songplay data
    songplay_data = []
    
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page']=='NextSong']

    # convert timestamp column to datetime
    t = df[['ts']].astype('datetime64[ms]')
    
    # insert time data records
    time_data = [ t['ts'], t['ts'].dt.hour, t['ts'].dt.day, t['ts'].dt.week, t['ts'].dt.month, t['ts'].dt.year, t['ts'].dt.weekday ] 
    column_labels = ('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    time_df = dict(zip(column_labels, time_data))
    time_df = pd.DataFrame(time_df)
    
    # remove dupplicates
    time_df = time_df.drop_duplicates(['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday'],keep='first').reset_index(drop=True)
    

    for i, row in time_df.iterrows():
        try:
            cur.execute(time_table_insert, list(row))
        except psycopg2.Error as e: 
            print("Error: inserting time data into the Database")
            print(e)

    # load user table
    user_df = df[['userId','firstName','lastName','gender','level']]
    # remove dupplicates
    user_df = user_df.drop_duplicates(['userId','firstName','lastName','gender','level'],keep='first').reset_index(drop=True)
    
    # insert user records
    for i, row in user_df.iterrows():
        try:
            cur.execute(user_table_insert, row)
        except psycopg2.Error as e: 
            print("Error: inserting user data into the Database")
            print(e)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None
            
            
        # insert songplay record
        songplay_data = [pd.Timestamp( row.ts, unit='ms') , row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent]
           
        # add error handler 
        try:
            cur.execute(songplay_table_insert, songplay_data)
        except psycopg2.Error as e:
            print("Error: inserting songplay data into the Database")
            print(e)

def process_data(cur, conn, filepath, func):
    
    """
    this function is used to get the total files to process and then 
    call the respective funtion to insert the data into the database.
    it requires 4 paramaters
    cur: cursor for database connections
    conn: parameters for database connection
    filepath: os path to the file to process
    func: the function to call to process the file (process_song_file | process_log_file ) 
    """
    
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    
    #set parameters for database connection
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()
    
    #call function process data for song files
    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    
    #call function process data for log files
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()