import googleapiclient.discovery
import json
import re
import pymongo
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import mysql.connector
import sqlalchemy
from sqlalchemy import create_engine
import pymysql
import pandas as pd
import numpy as np
import streamlit as st
import plotly.express as px
from PIL import Image
from streamlit_option_menu import option_menu
from streamlit_lottie import st_lottie

with open(r"C:\Users\sunil\OneDrive\Desktop\python guvi\youtube\Animation - 1719929877780.json",'r') as dd:
    data=json.load(dd)

icon=Image.open(r"C:\Users\sunil\OneDrive\Desktop\python guvi\youtube\utubeicon.png")
st.set_page_config(
    page_title="Youtube Data Harvesting | By SUNIL RAGAV",
    page_icon=icon,
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={'About': """# This dashboard app is created by *Sunil Ragav*!
                             Data has been fetched by API request"""}
)
st.sidebar.header(":wave: :red[**Hello! Welcome to the dashboard**]")



with st.sidebar:
   selected = option_menu("Menu", ["Home","Explore"], 
                icons=["house","search"],
                menu_icon= "menu-button-wide",
                default_index=0,
                styles={"nav-link": {"font-size": "20px", "text-align": "left", "margin": "-2px", "--hover-color": "red"},
                        "nav-link-selected": {"background-color": "red"}})
   

if selected == "Home":
    col1,col2,col3,col4,col5=st.columns([1,1,3,1,1])
    with col3:
        st.title(' :red[Youtube Data Harvesting]')
    col1, col2,col3 = st.columns([4,1,3])
    with col1:
        st.markdown("### :red[Domain :] Social Media")
        st.markdown("")
        st.markdown("### :red[Technologies used :] Python scripting, Data Collection, Streamlit, API integration, Data Management using SQL")
        st.markdown("")
        st.markdown("### :red[Overview :] This aims to develop a user-friendly Streamlit application that utilizes the Google API to extract information on a YouTube channel, stores it in a SQL database, and enables users to search for channel details and join tables to view data in the Streamlit app.")
    with col3:
        st_lottie(data, reverse=True, height=400, width=500, speed=1.5, loop=True, quality='high', key='utubeani')
        st.markdown(" ")
        st.markdown(" ")
        st.markdown(" ")




if selected=="Explore":
    st.markdown('<h1 style="color:red; text-align: center;">YouTube Data Harvesting</h1>', unsafe_allow_html=True)
    col1, col2 = st.columns(2)
    with col1:
        st.header(':red[Data collection zone]')
        st.write ('(Note:- This zone **collect data** by using channel id and **stored it in the :green[MongoDB] database**.)')
        channel_id = st.text_input('**Enter 11 digit channel_id**')
        st.write('''Get data and stored it in the MongoDB database to click below **:red['Get data and stored']**.''')
        Get_data = st.button('**Get data and stored**')


        def convert_duration(duration):
            regex = r'PT(\d+H)?(\d+M)?(\d+S)?'
            match = re.match(regex, duration)
            if not match:
                return '00:00:00'
            hours, minutes, seconds = match.groups()
            hours = int(hours[:-1]) if hours else 0
            minutes = int(minutes[:-1]) if minutes else 0
            seconds = int(seconds[:-1]) if seconds else 0
            return '{:02d}:{:02d}:{:02d}'.format(hours,minutes,seconds)

       
        # api_key = "AIzaSyCncfogPTJfiK8sUSmWTHjIqE8Co7cH7W4"
        api_key="AIzaSyB7LW1RhpHcsgusgKyrWi_C9arnnpC4VKc"
        
        api_service_name = "youtube"
        api_version = "v3"
        
        youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=api_key)
        request = youtube.channels().list(
        part="snippet, contentDetails, Statistics",
        id=channel_id,
        )

        response = request.execute()
        df = pd.DataFrame(response["items"])
        channel_data = response

        channel_name = channel_data['items'][0]['snippet']['title']
        channel_video_count = channel_data['items'][0]['statistics']['videoCount']
        channel_subscriber_count = channel_data['items'][0]['statistics']['subscriberCount']
        channel_view_count = channel_data['items'][0]['statistics']['viewCount']
        channel_description = channel_data['items'][0]['snippet']['description']
        channel_playlist_id = channel_data['items'][0]['contentDetails']['relatedPlaylists']['uploads']

        channel = {"Channel_Details": {
                        "Channel_Name": channel_name,
                        "Channel_Id": channel_id,
                        "Video_Count": channel_video_count,
                        "Subscriber_Count": channel_subscriber_count,
                        "Channel_Views": channel_view_count,
                        "Channel_Description": channel_description,
                        "Playlist_Id": channel_playlist_id
                    }
                }

        videos = {}
        request = youtube.playlistItems().list(
                            part='snippet,contentDetails',
                            playlistId=channel_playlist_id,
                            maxResults=50,
                            )
        response1 = request.execute()
        index = 1
        for item in response1['items']:
            request = youtube.videos().list(part='snippet, statistics, contentDetails', id=item['contentDetails']['videoId'])
            response3 = request.execute()
            
            for item1 in response3["items"]:
                convertedTime = convert_duration(item1["contentDetails"]["duration"])
                item1["contentDetails"]["duration"] = convertedTime
                requestComments = youtube.commentThreads().list(part='snippet', maxResults=2, textFormat="plainText", videoId = item['contentDetails']['videoId'] )
                response4 = requestComments.execute()
                comments = response4.get("items", [])
                commentsList = {}
            
            for i in range(0,len(comments)):
                commentsList[f"comment_Id_{i+1}"] = {
                    "authorChannelId" : comments[i]["snippet"]["topLevelComment"]["snippet"]["authorChannelId"],
                    "commentAuthor" : comments[i]["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                    "commentText" : comments[i]["snippet"]["topLevelComment"]["snippet"]["textDisplay"],
                    "commentPublishedAt" : comments[i]["snippet"]["topLevelComment"]["snippet"]["publishedAt"]
                }
                
                videos[f"Video_Id_{index}"] = {
                    "video_id" : item['contentDetails']['videoId'],
                    "videoPublishedAt" : item['contentDetails']['videoPublishedAt'],
                    "title" : item1["snippet"]["title"],
                    "description" : item1["snippet"]["description"],
                    "viewCount" : item1["statistics"]["viewCount"],
                    "likeCount" : item1["statistics"]["likeCount"],
                    "favoriteCount" : item1["statistics"]["favoriteCount"],
                    "commentCount" : item1["statistics"]["commentCount"],
                    "duration" : item1["contentDetails"]["duration"],
                    "comments" : commentsList
                }
                
                index += 1
        finalData = {**channel, **videos}
        
        
        client = pymongo.MongoClient("mongodb+srv://SUNIL:12345@cluster0.4p2eeut.mongodb.net/?retryWrites=true&w=majority")
        try:
            client.admin.command('ping')
            st.write("")
            st.write("")
            st.success("Pinged your deployment. You successfully connected to MongoDB!")
        except Exception as e:
            st.write(e)
        client=MongoClient("mongodb+srv://SUNIL:12345@cluster0.4p2eeut.mongodb.net/?retryWrites=true&w=majority")
        db=client.youtube
        records=db.youtube_data
        records.replace_one({'_id':channel_id},finalData,upsert=True)
        client.close()

    with col2:
        st.header(':red[Data Migrate zone]')
        st.write ('''(Note:- This zone specific channel data **Migrate to :blue[MySQL] database from  :green[MongoDB] database** depending on your selection,if unavailable your option first collect data.)''')
        client = pymongo.MongoClient("mongodb+srv://SUNIL:12345@cluster0.4p2eeut.mongodb.net/?retryWrites=true&w=majority")
        mydb = client['youtube']
        collection = mydb['youtube_data']
        document_names = []
        document_name = channel_id
        channelList = []
        videosList = []
        
        for document in collection.find():
            document_names.append(document["_id"])
            channelList.append(document['Channel_Details'])
        document_name = st.selectbox('**Select Channel name**', options = document_names, key='document_names')
        st.write('''Migrate to MySQL database from MongoDB database to click below **:red['Migrate to MySQL']**.''')
        Migrate = st.button('**Migrate to MySQL**')

        if 'migrate_sql' not in st.session_state:
            st.session_state_migrate_sql = False
        if Migrate or st.session_state_migrate_sql:
            st.session_state_migrate_sql = True
        
            result = collection.find_one({"_id": document_name})
            client.close()

            channel_details_to_sql = {
                "Channel_Name": result['Channel_Details']['Channel_Name'],
                "Channel_Id": result['Channel_Details']['Channel_Id'],
                "Video_Count": result['Channel_Details']['Video_Count'],
                "Subscriber_Count": result['Channel_Details']['Subscriber_Count'],
                "Channel_Views": result['Channel_Details']['Channel_Views'],
                "Channel_Description": result['Channel_Details']['Channel_Description'],
                "Playlist_Id": result['Channel_Details']['Playlist_Id']
            }

            playlist_tosql = {"Channel_Id": result['_id'], "Playlist_Id": result['Channel_Details']['Playlist_Id'],"Channel_Name": result['Channel_Details']['Channel_Name']}

            video_details_list = []
            comment_detail_list = []

            for i in range(1,len(result)-1):
                video_details_tosql = {
                    'Playlist_Id':result['Channel_Details']['Playlist_Id'],
                    'video_id': result[f"Video_Id_{i}"]['video_id'],
                    'title': result[f"Video_Id_{i}"]['title'],
                    'description': result[f"Video_Id_{i}"]['description'],
                    'videoPublishedAt': result[f"Video_Id_{i}"]['videoPublishedAt'],
                    'viewCount': result[f"Video_Id_{i}"]['viewCount'],
                    'likeCount': result[f"Video_Id_{i}"]['likeCount'],
                    'favoriteCount': result[f"Video_Id_{i}"]['favoriteCount'],
                    'commentCount': result[f"Video_Id_{i}"]['commentCount'],
                    'duration': result[f"Video_Id_{i}"]['duration'],
                }

                if(len(result[f"Video_Id_{i}"]['comments']) == 1):
                    comment_detail = {
                        'video_id': result[f"Video_Id_{i}"]['video_id'],
                        'Comment_Id': result[f"Video_Id_{i}"]['comments'][f"comment_Id_1"]['authorChannelId']['value'],
                        'commentText': result[f"Video_Id_{i}"]['comments'][f"comment_Id_1"]['commentText'],
                        'commentAuthor': result[f"Video_Id_{i}"]['comments'][f"comment_Id_1"]['commentAuthor'],
                        'commentPublishedAt': result[f"Video_Id_{i}"]['comments'][f"comment_Id_1"]['commentPublishedAt'],
                    }
                    comment_detail_list.append(comment_detail)
                else:
                    for j in range(1,3):
                        comment_detail = {
                            'video_id': result[f"Video_Id_{i}"]['video_id'],
                            'Comment_Id': result[f"Video_Id_{i}"]['comments'][f"comment_Id_{j}"]['authorChannelId']['value'],
                            'commentText': result[f"Video_Id_{i}"]['comments'][f"comment_Id_{j}"]['commentText'],
                            'commentAuthor': result[f"Video_Id_{i}"]['comments'][f"comment_Id_{j}"]['commentAuthor'],
                            'commentPublishedAt': result[f"Video_Id_{i}"]['comments'][f"comment_Id_{j}"]['commentPublishedAt'],
                        }
                        comment_detail_list.append(comment_detail)
                video_details_list.append(video_details_tosql)


            video_df = pd.DataFrame(video_details_list)
            channel_df = pd.DataFrame.from_dict(channel_details_to_sql, orient='index').T
            playlist_df = pd.DataFrame.from_dict(playlist_tosql, orient='index').T
            Comments_df = pd.DataFrame(comment_detail_list)
            st.success("Migrate to MySQL database from MongoDB database")

            mydb= mysql.connector.connect(
                host="localhost",
                user="root",
                password="",
            )
            mycursor = mydb.cursor()
            mycursor.execute("CREATE DATABASE IF NOT EXISTS youtube")
            mycursor = mydb.cursor(buffered=True)
            connection_str = f"mysql+mysqlconnector://root:@localhost/youtube"
            engine = create_engine(connection_str)

            channel_df.to_sql('channel', engine, if_exists='append', index=False,
                dtype = {"Channel_Name": sqlalchemy.types.VARCHAR(length=225),
                        "Channel_Id": sqlalchemy.types.VARCHAR(length=225),
                        "Video_Count": sqlalchemy.types.INT,
                        "Subscriber_Count": sqlalchemy.types.BigInteger,
                        "Channel_Views": sqlalchemy.types.BigInteger,
                        "Channel_Description": sqlalchemy.types.TEXT,
                        "Playlist_Id": sqlalchemy.types.VARCHAR(length=225),})

        
            playlist_df.to_sql('playlist', engine, if_exists='append', index=False,
                dtype = {"Channel_Id": sqlalchemy.types.VARCHAR(length=225),
                "Playlist_Id": sqlalchemy.types.VARCHAR(length=225),
                "Channel_Name": sqlalchemy.types.VARCHAR(length=225),})

        
            video_df.to_sql('video', engine, if_exists='append', index=False,
                dtype = {'Playlist_Id': sqlalchemy.types.VARCHAR(length=225),
                        'video_id': sqlalchemy.types.VARCHAR(length=225),
                        'title': sqlalchemy.types.VARCHAR(length=225),
                        'description': sqlalchemy.types.TEXT,
                        'videoPublishedAt': sqlalchemy.types.String(length=50),
                        'viewcount': sqlalchemy.types.BigInteger,
                        'likecount': sqlalchemy.types.BigInteger,
                        'dislikecount': sqlalchemy.types.INT,
                        'favoritecount': sqlalchemy.types.INT,
                        'commentcount': sqlalchemy.types.INT,
                        'duration': sqlalchemy.types.VARCHAR(length=1024),})

            Comments_df.to_sql('comments', engine, if_exists='append', index=False,
                dtype = {'video_id': sqlalchemy.types.VARCHAR(length=225),
                        'comment_Id': sqlalchemy.types.VARCHAR(length=225),
                        'commenttext': sqlalchemy.types.TEXT,
                        'commentAuthor': sqlalchemy.types.VARCHAR(length=225),
                        'commentPublishedAt': sqlalchemy.types.String(length=50),})
            mydb.commit()
            mycursor.close()
            mydb.close()



    ##################################


    # -----------------------------------------------------     /   Questions   /    ------------------------------------------------------------- #
    st.subheader(':violet[Channels Analysis ]')

    # Selectbox creation
    question_tosql = st.selectbox('**Select your Question**',
    ('1. What are the names of all the videos and their corresponding channels?',
    '2. Which channels have the most number of videos, and how many videos do they have?',
    '3. What are the top 10 most viewed videos and their respective channels?',
    '4. How many comments were made on each video, and what are their corresponding video names?',
    '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
    '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
    '7. What is the total number of views for each channel, and what are their corresponding channel names?',
    '8. What are the names of all the channels that have published videos in the year 2022?',
    '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
    '10. Which videos have the highest number of comments, and what are their corresponding channel names?'), key = 'collection_question')

    # Creat a connection to SQL
    connect_for_question = pymysql.connect(host='localhost', user='root', password='', db='youtube')
    cursor = connect_for_question.cursor()

    # Q1
    if question_tosql == '1. What are the names of all the videos and their corresponding channels?':
        cursor.execute("SELECT channel.Channel_Name, video.title FROM channel JOIN playlist JOIN video ON channel.Channel_Id = playlist.Channel_Id AND playlist.Playlist_Id = video.Playlist_Id;")
        result_1 = cursor.fetchall()
        df1 = pd.DataFrame(result_1, columns=['Channel_Name', 'title']).reset_index(drop=True)
        df1.index += 1
        st.dataframe(df1)

    # Q2
    elif question_tosql == '2. Which channels have the most number of videos, and how many videos do they have?':

        
            cursor.execute("SELECT Channel_Name, Video_Count FROM channel ORDER BY Video_Count DESC;")
            result_2 = cursor.fetchall()
            df2 = pd.DataFrame(result_2,columns=['Channel_Name','Video_Count']).reset_index(drop=True)
            df2.index += 1
            st.dataframe(df2)



    # Q3
    elif question_tosql == '3. What are the top 10 most viewed videos and their respective channels?':

        
            cursor.execute("SELECT channel.Channel_Name, video.title, video.viewCount FROM channel JOIN playlist ON channel.Channel_Id = playlist.Channel_Id JOIN video ON playlist.Playlist_Id = video.Playlist_Id ORDER BY video.viewCount DESC LIMIT 10;")
            result_3 = cursor.fetchall()
            df3 = pd.DataFrame(result_3,columns=['Channel_Name', 'title', 'viewcount']).reset_index(drop=True)
            df3.index += 1
            st.dataframe(df3)

        
    # Q4 
    elif question_tosql == '4. How many comments were made on each video, and what are their corresponding video names?':
        cursor.execute("SELECT channel.Channel_Name, video.title, video.commentCount FROM channel JOIN playlist ON channel.Channel_Id = playlist.Channel_Id JOIN video ON playlist.Playlist_Id = video.Playlist_Id;")
        result_4 = cursor.fetchall()
        df4 = pd.DataFrame(result_4,columns=['Channel_Name', 'title', 'commentCount']).reset_index(drop=True)
        df4.index += 1
        st.dataframe(df4)

    # Q5
    elif question_tosql == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
        cursor.execute("SELECT channel.Channel_Name, video.title, video.likeCount FROM channel JOIN playlist ON channel.Channel_Id = playlist.Channel_Id JOIN video ON playlist.Playlist_Id = video.Playlist_Id ORDER BY video.likeCount DESC;")
        result_5= cursor.fetchall()
        df5 = pd.DataFrame(result_5,columns=['Channel_Name', 'title', 'likeCount']).reset_index(drop=True)
        df5.index += 1
        st.dataframe(df5)

    # Q6
    elif question_tosql == '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
        st.write('**Note:- In November 2021, YouTube removed the public dislike count from all of its videos.**')
        cursor.execute("SELECT channel.Channel_Name, video.title, video.likeCount FROM channel JOIN playlist ON channel.Channel_Id = playlist.Channel_Id JOIN video ON playlist.Playlist_Id = video.Playlist_Id ORDER BY video.likeCount DESC;")
        result_6= cursor.fetchall()
        df6 = pd.DataFrame(result_6,columns=['Channel_Name', 'title', 'likeCount']).reset_index(drop=True)
        df6.index += 1
        st.dataframe(df6)

    # Q7
    elif question_tosql == '7. What is the total number of views for each channel, and what are their corresponding channel names?':

        
            cursor.execute("SELECT Channel_Name, Channel_Views FROM channel ORDER BY Channel_Views DESC;")
            result_7= cursor.fetchall()
            df7 = pd.DataFrame(result_7,columns=['Channel_Name', 'Channel_Views']).reset_index(drop=True)
            df7.index += 1
            st.dataframe(df7)
        
    # Q8
    elif question_tosql == '8. What are the names of all the channels that have published videos in the year 2022?':
        cursor.execute("SELECT channel.Channel_Name, video.title, video.videoPublishedAt FROM channel JOIN playlist ON channel.Channel_Id = playlist.Channel_Id JOIN video ON playlist.Playlist_Id = video.Playlist_Id  WHERE EXTRACT(YEAR FROM videoPublishedAt) = 2022;")
        result_8= cursor.fetchall()
        df8 = pd.DataFrame(result_8,columns=['Channel_Name','title', 'Year 2022 only']).reset_index(drop=True)
        df8.index += 1
        st.dataframe(df8)

    # Q9
    elif question_tosql == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
        cursor.execute("SELECT channel.Channel_Name, TIME_FORMAT(SEC_TO_TIME(AVG(TIME_TO_SEC(TIME(video.duration)))), '%H:%i:%s') AS duration  FROM channel JOIN playlist ON channel.Channel_Id = playlist.Channel_Id JOIN video ON playlist.Playlist_Id = video.Playlist_Id GROUP by Channel_Name ORDER BY duration DESC ;")
        result_9= cursor.fetchall()
        df9 = pd.DataFrame(result_9,columns=['Channel_Name','Average duration of videos (HH:MM:SS)']).reset_index(drop=True)
        df9.index += 1
        st.dataframe(df9)

    # Q10
    elif question_tosql == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
        cursor.execute("SELECT channel.Channel_Name, video.title, video.commentcount FROM channel JOIN playlist ON channel.Channel_Id = playlist.Channel_Id JOIN video ON playlist.Playlist_Id = video.Playlist_Id ORDER BY video.commentcount DESC;")
        result_10= cursor.fetchall()
        df10 = pd.DataFrame(result_10,columns=['Channel_Name','title', 'commentcount']).reset_index(drop=True)
        df10.index += 1
        st.dataframe(df10)

    # SQL DB connection close
    connect_for_question.close()