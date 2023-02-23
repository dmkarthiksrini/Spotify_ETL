# import necesary Libraries

import pandas as pd
import sqlalchemy # ORM(Object Relational Mapper)
from sqlalchemy.orm import sessionmaker
import requests
import json
import datetime as dt
from datetime import datetime, timedelta
import sqlite3

# sqlalchemy is an ORM which allows us to access data directly from Python WHithout using any SQL

print("SPOTIFY EXTRACTION!!")


# 4. Create a dashboard

# 1. Extract data from Spotify API

# To extract Data from spotify we need to have a spotify account which is free to create
# Once you have created the account you need to create a spotify app and get the User_id and client secret in here Token
# You can get the token from the spotify developer website(API) (https://developer.spotify.com/console/get-recently-played/)
# You can get the user_id from the spotify website (https://www.spotify.com/account/overview/)

DATABASE_LOCATION = "sqlite:///my_played_tracks.sqlite"
USER_ID = "dmkarthiksrini55"
TOKEN = "BQAsklNY4LTjI0tKQ2d37ZlDXnPE6Q5Bn6x4fgRjxXiSiigJmvZUksy-cYeVZNqzuqkzH2PPg_JMkgJ-TpY17zCEUep9jWB8iS3dauh0rfyzr9VDAjXGL1rMYZqG8AVPuZip-waY4a163jrCvclX5v9rYqaOIrMILIjHQ0CwIFlrT0DbCpUXrJ3njL7CAg"


# 2. Transform the data

def check_if_valid_data(df: pd.DataFrame) -> bool:
    # check if dataframe is empty
    if df.empty():
        print("No songs downloaded. Finishing execution")
        return False

    # Primary Key Check (to validate the Primary Key)
    # In our Spotify Data Played at is the Primary Key since we can hear only one song at a time
    if pd.Series(df["played_at"]).is_unique:
        pass
    else:
        raise Exception("Primary Key Check is violated")

    # Check for nulls
    if df.isnull().values.any():
        raise Exception("Null values found")

    # Check that all timestamps are of yesterday's date
    yesterday = datetime.now() - timedelta(days=1)
    yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)

    timestamps = df["timestamp"].tolist()
    for timestamp in timestamps:
        if datetime.strptime(timestamp, "%Y-%m-%d") != yesterday:
            raise Exception("At least one of the returned songs does not come from within the last 24 hours")

    return True


if __name__ == '__main__':

    headers = {
        "Accept" : "application/json",
        "Content-Type" : "application/json",
        "Authorization" : "Bearer {token}".format(token=TOKEN)
    }

    today = dt.datetime.now()
    yesterday = today - dt.timedelta(days=1)
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000

    r = requests.get("https://api.spotify.com/v1/me/player/recently-played?after={time}".format(time=yesterday_unix_timestamp), headers=headers)

    data = r.json()

    print(data)

    song_names = []
    artist_names = []
    played_at_list = []
    timestamps = []

    for song in data["items"]:
        song_names.append(song["track"]["name"])
        artist_names.append(song["track"]["album"]["artists"][0]["name"])
        played_at_list.append(song["played_at"])
        timestamps.append(song["played_at"][0:10])

    song_dict = {
        "song_name" : song_names,
        "artist_name" : artist_names,
        "played_at" : played_at_list,
        "timestamp" : timestamps
    }

    song_df = pd.DataFrame(song_dict, columns= ["song_name", "artist_name", "played_at","timestamp"])

# 3. Load the data into a database

# Create a database connection
# Create a database engine

engine = sqlalchemy.create_engine(DATABASE_LOCATION)
conn = sqlite3.connect('my_played_tracks.sqlite')
cursor = conn.cursor()

# Create a table in the database

sql_query = """
CREATE TABLE IF NOT EXISTS my_played_tracks(
    song_name VARCHAR(200),
    artist_name VARCHAR(200),
    played_at VARCHAR(200),
    timestamp VARCHAR(200),
    CONSTRAINT primary_key_constraint PRIMARY KEY (played_at)
)
"""

cursor.execute(sql_query)
print("Opened database successfully")

# Insert the data into the database

try:
    song_df.to_sql ("my_played_tracks", engine, index=False, if_exists='replace')
except:
    print("Data already exists in the database")

conn.close()
print("Close database successfully")

    # Validate
    
    #if check_if_valid_data(song_df):
    #    print("Data valid, proceed to Load stage")


print(song_df)