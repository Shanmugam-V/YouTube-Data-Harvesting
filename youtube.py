
from googleapiclient.discovery import build
import pymongo
import pymysql
import pandas as pd
from datetime import datetime
import numpy as np
import datetime
import re
import mysql.connector
import streamlit as st

def API_connection():
    api="AIzaSyA-jIa8FxWSU6gegC-IASOCWvf8Fo1I5kY"

    api_service_name = "youtube"
    api_version = "v3"

    youtube=build(api_service_name,api_version,developerKey=api)

    return youtube

youtube=API_connection()

def get_channel_info(channel_Id):
        Request=youtube.channels().list(
                part="snippet,contentDetails,statistics",
                id=channel_Id
        )

        response=Request.execute()

        for i in response['items']:
                data=dict(channel_Name=i["snippet"]["title"],
                        channel_id=i["id"],
                        Subscribres=i["statistics"]["subscriberCount"],
                        Total_Videos=i["statistics"]["videoCount"],
                        Total_Views=i["statistics"]["viewCount"],
                        Channel_Description=i["snippet"]["description"],
                        playlist_ID=i["contentDetails"]["relatedPlaylists"],
                         )
                return data
        
# Chn_details=get_channel_info (channel_Id)

def get_videos_id(channel_Id):
        
        Videos_ID=[]
        Request=youtube.channels().list(id=channel_Id, part="contentDetails")
        response=Request.execute()
        playlist_id=response['items'][0]["contentDetails"]["relatedPlaylists"]["uploads"]
        next_Page=None
        while True:


                request = youtube.playlistItems().list(
                        part="snippet",
                        maxResults=50,
                        playlistId=playlist_id,
                        pageToken=next_Page

                        )
                response1 = request.execute()

                #print(response)

                for i in range(len(response1['items'])):


                        Videos_ID.append (response1['items'][i]["snippet"]["resourceId"]["videoId"])


                next_Page = response1.get("nextPageToken")
                if next_Page is None:
                        break
        return Videos_ID




def get_Video_info(Video_ids):
        Video_data=[]
        for video_ID in Video_ids:
                request = youtube.videos().list(
                                part="snippet,contentDetails,statistics",
                                id=video_ID
                        )

                response = request.execute()
                print(response)

                for item in response ["items"]:
                        data=dict(Channel_Name=item["snippet"]["channelTitle"],
                        Channenl_ID=item["snippet"]["channelId"],
                        Video_ID=item["id"],
                        Title=item["snippet"]["title"],
                        Thumbnails=item["snippet"]["thumbnails"]["default"]['url'],
                        Description=item["snippet"]["description"],
                        Published=item["snippet"]["publishedAt"],
                        Duration=item["contentDetails"]["duration"],
                        Views=item["statistics"].get("viewCount"),
                        Likes=item["statistics"].get("likeCount"),
                        comments=item["statistics"].get("commentCount"),
                        Favorite_Count=item["statistics"]["favoriteCount"],
                        Defination=item["contentDetails"]["definition"],
                        caption_status=item["contentDetails"]["caption"],
                        Tags=item["snippet"].get("Tags")

                        )

                Video_data.append(data)

        return Video_data

def get_comments(Video_ids):
        Comment_data=[]
        try:
                for video_ID in Video_ids:
                        request = youtube.commentThreads().list(
                        part="snippet",
                        videoId=video_ID,
                        maxResults=50
                         )
                        response = request.execute()



                        for item in response ["items"]:
                                data=dict(comment_Id=item["snippet"]["topLevelComment"]["id"],
                                        Video_ID=item["snippet"]["topLevelComment"]["snippet"]["videoId"],
                                        Comment_Text=item["snippet"]["topLevelComment"]["snippet"]["textDisplay"],
                                        comment_Author_Name=item["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                                        comment_publish_time=item["snippet"]["topLevelComment"]["snippet"]["publishedAt"]
                                        )
                                Comment_data.append(data)

        except:
                pass
    

        return Comment_data
client=pymongo.MongoClient("mongodb://localhost:27017/")
db = client['youtube_data']

def Channel_details(Channel_ID):
    Channel_details=get_channel_info(Channel_ID)
    Video_ID=get_videos_id(Channel_ID)
    Vid_Details=get_Video_info(Video_ID)
    Cmnt_details=get_comments(Video_ID)

    Collection1=db["Channel_details"]
    Collection1.insert_one({"channel_information":Channel_details,
                            "Video_information":Vid_Details,"Comment_Information":Cmnt_details})

    return "upload Successfully"

#Upload=Channel_details("UCD4fBiLu_tbf_XH73Pp9vxQ")



#table creation & value inserted in SQL
def value_inserted_in_SQl (channel_Id):
        mydb = pymysql.connect(
                        user='root',
                        host='127.0.0.1',
                        password="shanmugam",
                        port=3306,
                        database='youtube_data'
                        )
        mycursor=mydb.cursor()


        query = '''create table if not exists channel (channel_Name VARCHAR(100), channel_id VARCHAR(100), Subscribres bigint, Total_videos_1 bigint, Total_Views bigint, Channel_Description text)
                '''





        mycursor.execute(query)

        mydb.commit()


        print("channel table created")

        #channel_Id="UCZEjVppMa1DL7AHt___xR-Q"
        Collection1=db["Channel_details"]
        data = Collection1.find_one({"channel_information.channel_id":channel_Id})
        #print(data.keys())


        channel_df=pd.DataFrame(data["channel_information"],index=[0])      

        video_df =pd.DataFrame(data["Video_information"])

        comment_df =pd.DataFrame(data["Comment_Information"])



        insert_query=''' insert into channel(channel_Name,
                                        channel_id,
                                        Subscribres,
                                        Total_videos_1,
                                        Total_Views,
                                        Channel_Description)
                                        
                                        values(%s,%s,%s,%s,%s,%s)'''

        subscribers = int(data["channel_information"]["Subscribres"])

        values = (data["channel_information"]["channel_Name"],
                data["channel_information"]["channel_id"],
                subscribers,
                data["channel_information"]["Total_Videos"],
                data["channel_information"]["Total_Views"],
                data["channel_information"]["Channel_Description"])



        mycursor.execute(insert_query,values)
        mydb.commit()

        query = '''create table if not exists videos_1 (Channel_Name VARCHAR(100), Channenl_ID VARCHAR(100), Video_ID VARCHAR(100), Title text, Thumbnails VARCHAR(255), Description text, Published VARCHAR(50), Duration VARCHAR(100), Views bigint, Likes bigint, comments bigint, Favorite_Count int, Defination varchar(10), caption_status varchar(50), Tags text)'''

        mycursor.execute(query)

        mydb.commit()


        print("Video table created")





        insert_query=''' insert into videos_1(Channel_Name,
                                                Channenl_ID,
                                                Video_ID,
                                                Title,
                                                Thumbnails,
                                                Description,
                                                Published,
                                                Duration,
                                                Views,
                                                Likes,
                                                comments,
                                                Favorite_Count,
                                                Defination,
                                                caption_status,
                                                Tags)
                                                
                                                values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''



        for index, row in video_df.iterrows():
    
                values = tuple(row.values)
                mycursor.execute(insert_query, values)
                mydb.commit()

        query = '''create table IF NOT EXISTS comments_1 (comment_Id VARCHAR(100), Video_ID VARCHAR(100), Comment_Text text, comment_Author_Name VARCHAR(150), comment_publish_time VARCHAR(50))'''
                



        mycursor.execute(query)

        mydb.commit()


        print("comment table created")

        
        #CMNT_ID="UgzMPdWa-F5cUdkwn-h4AaABAg"


        insert_query = '''INSERT INTO comments_1 (comment_Id, 
                                                Video_ID,  
                                                Comment_Text,
                                                comment_Author_Name,
                                                comment_publish_time
                                                )
                        VALUES (%s, %s, %s, %s, %s)'''


        for index,row in comment_df.iterrows():
                
                #print(row.values)
                values = tuple(row.values)
                mycursor.execute(insert_query, values)
                mydb.commit()

    
#Value_added= value_inserted_in_SQl()

#streamlit part

import streamlit as st

client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client['youtube_data']
collection1 = db['Channel_details']

def get_channel_names():
    channels = collection1.distinct("channel_information.channel_Name")
    return channels

def Channel_details(channel_id):
    Channel_details = get_channel_info(channel_id)
    Video_ID = get_videos_id(channel_id)
    Vid_Details = get_Video_info(Video_ID)
    Cmnt_details = get_comments(Video_ID)

    Collection1 = db["Channel_details"]
    Collection1.insert_one({
        "channel_information": Channel_details,
        "Video_information": Vid_Details,
        "Comment_Information": Cmnt_details
    })

    return "Uploaded Successfully"


mydb = pymysql.connect(
    user='root',
    host='127.0.0.1',
    password="shanmugam",
    port=3306,
    database='youtube_data'
)

# Create cursor
mycursor = mydb.cursor()

# Function to execute SQL query and return DataFrame
def execute_query_and_return_df(query):
    mycursor.execute(query)
    mydb.commit()
    result = mycursor.fetchall()
    df = pd.DataFrame(result)
    return df

st.title("YouTube Data Analysis")
st.sidebar.header("Channel Selection")

# Main content

with st.sidebar:
    #st.title("YouTube Data Analysis")
    #st.markdown("---")
    #st.header("Channel Selection")
    
    channel_Id=st.text_input("Enter channel ID")
    if channel_Id and st.button("Data to MongoDB"):
         Message=Channel_details(channel_Id)
         st.write(Message)

         channels = get_channel_names()
         selected_channel = st.selectbox("Select Channel:", channels)
         st.write("You selected:", selected_channel)


    if channel_Id and st.sidebar.button("Fetch Data and Insert into MySQL"):
                channel_data = value_inserted_in_SQl(channel_Id)
                st.sidebar.write("Data transferred to MySQL.")

#channels = get_channel_names()
#channel_names = st.sidebar.selectbox("Select Channel:", channels)
#st.sidebar.write("You selected:", channel_names)  

#st.write("You selected:", channel_names)

#st.markdown("---")
st.header("Analysis Options")
analysis_option = st.selectbox("Select an analysis:", 
                                   ["Videos and their corresponding channels",
                                    "Channels with most number of videos",
                                    "Top 10 most viewed videos and their channels",
                                    "Number of comments per video",
                                    "Videos with the highest number of likes",
                                    "Total likes and dislikes per video",
                                    "Total views per channel",
                                    "Channels that published videos in 2022",
                                    "Average duration of videos per channel",
                                    "Videos with the highest number of comments"])
#st.markdown("---")
#st.caption("Scripting")
#st.caption("Data Collection")
#st.caption("MongoDB")x``
#st.caption("YouTube")
#st.caption("Data Handling with MongoDB and SQL")

# Execute selected analysis for the selected channel ID

if analysis_option == "Videos and their corresponding channels":
    query = """SELECT Title AS videos, Channel_Name AS channelname FROM videos_1"""
    df = execute_query_and_return_df(query)
    st.write(df)

elif analysis_option == "Channels with most number of videos":
    query = """SELECT channel_name AS channelname, Total_videos_1 AS no_videos FROM channel ORDER BY Total_videos_1 DESC"""
    df = execute_query_and_return_df(query)
    st.write(df)

elif analysis_option == "Top 10 most viewed videos and their channels":
    query = """SELECT views AS views, channel_name AS channelname, title AS videotitle FROM videos_1 
            WHERE views IS NOT NULL ORDER BY views DESC LIMIT 10"""
    df = execute_query_and_return_df(query)
    st.write(df)

elif analysis_option == "Number of comments per video":
    query = """SELECT comments AS no_comments, title AS videotitle FROM videos_1 
            WHERE comments IS NOT NULL ORDER BY views DESC"""
    df = execute_query_and_return_df(query)
    st.write(df)

elif analysis_option == "Videos with the highest number of likes":
    query = """SELECT title AS videotitle, channel_name AS channelname, Likes AS likecount FROM videos_1 
            ORDER BY likes DESC;"""
    df = execute_query_and_return_df(query)
    st.write(df)

elif analysis_option == "Total likes and dislikes per video":
    query = """SELECT Likes AS likecount, title AS videotitle FROM videos_1"""
    df = execute_query_and_return_df(query)
    st.write(df)

elif analysis_option == "Total views per channel":
    query = """SELECT channel_name AS channelname,Views AS total_views FROM videos_1"""
    df = execute_query_and_return_df(query)
    st.write(df)

elif analysis_option == "Channels that published videos in 2022":
    query = """SELECT title AS videotitle, Published AS Vido_realesed_date, channel_name AS channelname FROM videos_1 
            WHERE YEAR(Published) = 2022"""
    df = execute_query_and_return_df(query)
    st.write(df)

elif analysis_option == "Average duration of videos per channel":
    query = """SELECT channel_name AS channelname, 
       AVG(TIME_TO_SEC(Duration)) AS avg_duration_seconds 
FROM videos_1 
GROUP BY channel_name;"""
    df = execute_query_and_return_df(query)
    st.write(df)
elif analysis_option == "Videos with the highest number of comments":
    query = """SELECT title AS video_title, channel_name AS channelname, comments AS comments FROM videos_1
            WHERE comments IS NOT NULL ORDER BY comments DESC"""
    df = execute_query_and_return_df(query)
    st.write(df)