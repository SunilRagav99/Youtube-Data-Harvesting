import googleapiclient.discovery
from googleapiclient.errors import HttpError
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
st.markdown("""
    <style>
    .center-title {
        text-align: center;
        color: red;
    }
    </style>
    <h1 class="center-title">Youtube Data Harvesting</h1>
    """, unsafe_allow_html=True)
st.write(" ")
st.write(" ")
st.write(" ")

selected = option_menu(None, ["Home","Data Collection Zone","Data Migration Zone","channel Analysis"], 
            icons=["house",],
            menu_icon= "menu-button-wide",
            default_index=0,
            orientation="horizontal",
            styles={"nav-link": {"font-size": "20px", "text-align": "left", "margin": "4px", "--hover-color": "red","padding":"5px"},
                    "nav-link-selected": {"background-color": "red"},"margin":"2px"})


if selected == "Home":
    col1, col2,col3 = st.columns([4,1,3])
    with col1:
        st.markdown("")
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



if selected=="Data Collection Zone":
    col1,col3= st.columns([5,2])
    with col1:
        st.header(':red[Data collection zone]')
        st.write ('(Note:- This zone **collect data** by using channel id and **stored it in the :green[MongoDB] database**.)')
        take = st.text_input('**Enter 11 digit channel_id**',)
        channel_id=take
        st.write('''Get data and stored it in the MongoDB database to click below **:red['Get data and stored']**.''')
        Get_data = st.button('**Get data and stored**')

    with col3:
        st_lottie(data, reverse=True, height=400, width=500, speed=1.5, loop=True, quality='high', key='utubeani')
        st.markdown(" ")
        st.markdown(" ")
        st.markdown(" ")

        

    with col1:
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

        if 'items' not in response:
         st.error("No data found or API quota exceeded.")
        else:
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
                    try:
                        response4 = requestComments.execute()
                        comments = response4.get('items', [])
                        commentsList = {}
                        if comments:

                            for i in range(len(comments)):
                                comment_snippet = comments[i].get("snippet", {}).get("topLevelComment", {}).get("snippet", {})
                                print(comment_snippet)
                                commentsList[f"comment_Id_{i+1}"] = {
                                    "authorChannelId": comment_snippet.get("authorChannelId", {}).get("value"),
                                    "commentAuthor": comment_snippet.get("authorDisplayName"),
                                    "commentText": comment_snippet.get("textDisplay"),
                                    "commentPublishedAt": comment_snippet.get("publishedAt")
                                }

                    except HttpError as e:
                        if e.resp.status == 403 and 'commentsDisabled' in str(e):
                            st.info(f"Comments are disabled for video ID: {item['contentDetails']['videoId']}")
                            commentsList = {} 
                            
                            

                    
                    
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
            if Get_data and take:
                    st.success("Data collected successfully using Google API requests")
                    
                    
                    client = pymongo.MongoClient("mongodb+srv://SUNIL:12345@cluster0.4p2eeut.mongodb.net/?retryWrites=true&w=majority")
                    try:
                        client.admin.command('ping')
                        
                        st.success("Pinged your deployment. You successfully connected to MongoDB!")
                    except Exception as e:
                        st.write(e)
                    client=MongoClient("mongodb+srv://SUNIL:12345@cluster0.4p2eeut.mongodb.net/?retryWrites=true&w=majority")
                    db=client.youtube
                    records=db.youtube_data
                    records.replace_one({'_id':channel_id},finalData,upsert=True)
                    client.close()
                    st.success(f"Data stored for the channel named {channel_name}")

if selected=="Data Migration Zone":
        col1,col2=st.columns([4,2])
        with col1:
            st.header(':red[Data Migrate zone]')
            st.write ('''(Note:- This zone specific channel data **Migrate to :blue[MySQL] database from  :green[MongoDB] database** depending on your selection,if unavailable your option first collect data.)''')
            client = pymongo.MongoClient("mongodb+srv://SUNIL:12345@cluster0.4p2eeut.mongodb.net/?retryWrites=true&w=majority")
            mydb = client['youtube']
            collection = mydb['youtube_data']
            document_names = []
            
            channelList = []
            videosList = []
            
            for document in collection.find():
                document_names.append(document["_id"])
                
                channelList.append(document.get('Channel_Details'))
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

                    if(len(result[f"Video_Id_{i}"]['comments']) == 2):
                    
                        for j in range(1,3):
                            comment_detail = {
                                'video_id': result[f"Video_Id_{i}"]['video_id'],
                                'Comment_Id': result[f"Video_Id_{i}"]['comments'][f"comment_Id_{j}"]['authorChannelId'],
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
                # st.dataframe(Comments_df)
                

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
                mycursor.execute("use youtube")




                mycursor.execute("""CREATE TABLE IF NOT EXISTS channel (
                            channel_id VARCHAR(255) PRIMARY KEY,
                            channel_name VARCHAR(255) UNIQUE,
                            Subscriber_count BIGINT,
                            Video_count INT,
                            Channel_views BIGINT,
                            Channel_Description TEXT,
                            Playlist_Id VARCHAR(255)
                        )
                    """)
                try:
                    for i, row in channel_df.iterrows():
                        sql = "INSERT INTO channel (channel_id, channel_name, Subscriber_count,Video_count,Channel_views,Channel_Description,Playlist_Id) VALUES (%s, %s, %s, %s, %s, %s,%s)"
                        values = (row['Channel_Id'], row['Channel_Name'],row['Subscriber_Count'],row['Video_Count'],row['Channel_Views'],row['Channel_Description'],row['Playlist_Id'])
                        mycursor.execute(sql, values)
                        mydb.commit()
                except mysql.connector.IntegrityError as e:
                    try:
                        sql_update = "UPDATE channel SET Channel_Name = %s, Subscriber_Count = %s, Video_Count = %s, Channel_Views = %s, Channel_Description = %s, Playlist_Id = %s WHERE Channel_Id = %s"
                        values_update = (row['Channel_Name'], row['Subscriber_Count'], row['Video_Count'], row['Channel_Views'], row['Channel_Description'], row['Playlist_Id'], row['Channel_Id'])
                        mycursor.execute(sql_update, values_update)
                        mydb.commit()
                        st.success("Updated channel data for Channel Table")
                    except mysql.connector.Error as update_err:
                        st.error(f"Error updating channel data for {row['Channel_Id']}: {update_err}")
                        




                mycursor.execute("""
                    CREATE TABLE IF NOT EXISTS playlist (
                        Channel_Id VARCHAR(255) PRIMARY KEY,
                        Channel_Name VARCHAR(255) UNIQUE,
                        Playlist_Id VARCHAR(255)
                    )
                """)
                try:
                    for i, row in playlist_df.iterrows():
                        sql_insert = "INSERT INTO playlist (Channel_Id, Channel_Name, Playlist_Id) VALUES (%s, %s, %s)"
                        values_insert = (row['Channel_Id'], row['Channel_Name'], row['Playlist_Id'])
                        mycursor.execute(sql_insert, values_insert)
                        mydb.commit()
                        
                except mysql.connector.IntegrityError as e:
                    try:
                        sql_update = "UPDATE playlist SET Channel_Name = %s, Playlist_Id = %s WHERE Channel_Id = %s"
                        values_update = (row['Channel_Name'], row['Playlist_Id'], row['Channel_Id'])
                        mycursor.execute(sql_update, values_update)
                        mydb.commit()
                        st.success(f"Updated channel data for Playlist Table")
                        
                    except mysql.connector.Error as update_err:
                        st.error(f"Error updating channel data for {row['Channel_Id']}: {update_err}")
                        
                    


                mycursor.execute("""
                                CREATE TABLE IF NOT EXISTS video (
                                    Video_Id VARCHAR(255) PRIMARY KEY,
                                    Playlist_Id VARCHAR(255),
                                    Title VARCHAR(255),
                                    Description TEXT,
                                    videoPublishedAt VARCHAR(50),
                                    view_count BIGINT,
                                    like_count BIGINT,
                                    fav_count INT,
                                    comment_count INT,
                                    Duration VARCHAR(225)
                                                    )
                                                    """)
                try:
                    for i, row in video_df.iterrows():
                        sql_insert =  """
                        INSERT INTO video (
                            Video_Id, Playlist_Id, Title, Description, videoPublishedAt, view_count, like_count, fav_count, comment_count, Duration
                                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                                                                                """
                        
                        values_insert = (
                            row['video_id'], row['Playlist_Id'], row['title'], row['description'],
                            row['videoPublishedAt'], row['viewCount'], row['likeCount'],
                            row['favoriteCount'], row['commentCount'], row['duration']
                        )
                    

                        mycursor.execute(sql_insert, values_insert)
                        mydb.commit()
                except mysql.connector.IntegrityError as e:
                    try:
                        sql_update = """UPDATE video SET
                        Playlist_Id = %s,
                        Title = %s,
                        Description = %s,
                        videoPublishedAt = %s,
                        view_count = %s,
                        like_count = %s,
                        fav_count = %s,
                        comment_count = %s,
                        Duration = %s
                        WHERE Video_Id = %s"""
                        values_update = ( row['Playlist_Id'], row['title'], row['description'], row['videoPublishedAt'],
                                        row['viewCount'], row['likeCount'], row['favoriteCount'], row['commentCount'],
                                        row['duration'], row['video_id']
                                            ) 
                                        
                        mycursor.execute(sql_update, values_update)
                        mydb.commit()
                        st.success("Updated channel data for Video Table")
                        
                    except mysql.connector.Error as update_err:
                        st.error("Error updating channel data for Video Table")
                        
            






                
                mycursor.execute("""
                                CREATE TABLE IF NOT EXISTS comment (
                                    Video_Id VARCHAR(255),
                                    Comment_Id VARCHAR(255) ,
                                    Comment_Text Text PRIMARY KEY,
                                    Comment_Author VARCHAR(255),
                                    CommentPublishedAt VARCHAR(225)
                                                    )
                                                    """)
                try:
                    for i, row in Comments_df.iterrows():
                        sql_insert =  """
                        INSERT INTO comment (
                            Video_Id, Comment_Id, Comment_Text, Comment_Author, CommentPublishedAt) VALUES (%s, %s, %s, %s, %s)
                                                                                                """
                        
                        values_insert = (
                            row['video_id'], row['Comment_Id'], row['commentText'], row['commentAuthor'],
                            row['commentPublishedAt']
                        )
                    

                        mycursor.execute(sql_insert, values_insert)
                        mydb.commit()
                except mysql.connector.IntegrityError as e:
                    try:
                        sql_update = """UPDATE comment SET
                        Video_Id = %s,
                        Comment_Id = %s,
                        Comment_Author = %s,
                        CommentPublishedAt = %s
                        WHERE Comment_Text = %s 
                        """
                        values_update = ( row['video_id'], row['Comment_Id'], row['commentAuthor'],
                            row['commentPublishedAt'], row['commentText']
                                            ) 
                                        
                        mycursor.execute(sql_update, values_update)
                        mydb.commit()
                        st.success("Updated channel data for Comment Table")
                        
                    except mysql.connector.Error as update_err:
                        # st.error(f"Error updating channel data for {row['video_id']}: {update_err}")
                        st.info("updated comments")
                        


        

                mycursor.close()
                mydb.close()
        
        with col2:
            st_lottie(data, reverse=True, height=400, width=500, speed=1.5, loop=True, quality='high', key='utubeani')
            st.markdown(" ")
            st.markdown(" ")
            st.markdown(" ")


    ##################################

if selected=="channel Analysis":
    st.subheader(':red[Youtube Channels Analysis: ]')
    col1,col2=st.columns([4,2])
    # -----------------------------------------------------     /   Questions   /    ------------------------------------------------------------- #
    
    with col1:
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
        '10. Which videos have the highest number of comments, and what are their corresponding channel names?',
        '11. What are the names of all the channels that have published videos in the year 2024?'), key = 'collection_question')

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

            
                cursor.execute("SELECT channel.Channel_Name, video.title, video.view_count FROM channel JOIN playlist ON channel.Channel_Id = playlist.Channel_Id JOIN video ON playlist.Playlist_Id = video.Playlist_Id ORDER BY video.view_count DESC LIMIT 10;")
                result_3 = cursor.fetchall()
                df3 = pd.DataFrame(result_3,columns=['Channel_Name', 'title', 'viewcount']).reset_index(drop=True)
                df3.index += 1
                st.dataframe(df3)

            
        # Q4 
        elif question_tosql == '4. How many comments were made on each video, and what are their corresponding video names?':
            cursor.execute("SELECT channel.Channel_Name, video.title, video.comment_count FROM channel JOIN playlist ON channel.Channel_Id = playlist.Channel_Id JOIN video ON playlist.Playlist_Id = video.Playlist_Id;")
            result_4 = cursor.fetchall()
            df4 = pd.DataFrame(result_4,columns=['Channel_Name', 'title', 'commentCount']).reset_index(drop=True)
            df4.index += 1
            st.dataframe(df4)

        # Q5
        elif question_tosql == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
            cursor.execute("SELECT channel.Channel_Name, video.title, video.like_Count FROM channel JOIN playlist ON channel.Channel_Id = playlist.Channel_Id JOIN video ON playlist.Playlist_Id = video.Playlist_Id ORDER BY video.like_Count DESC;")
            result_5= cursor.fetchall()
            df5 = pd.DataFrame(result_5,columns=['Channel_Name', 'title', 'likeCount']).reset_index(drop=True)
            df5.index += 1
            st.dataframe(df5)

        # Q6
        elif question_tosql == '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
            st.write('**Note:- In November 2021, YouTube removed the public dislike count from all of its videos.**')
            cursor.execute("SELECT channel.Channel_Name, video.Title, video.like_count FROM channel JOIN playlist ON channel.Channel_Id = playlist.Channel_Id JOIN video ON playlist.Playlist_Id = video.Playlist_Id ORDER BY video.like_Count DESC;")
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
            cursor.execute("SELECT channel.Channel_Name, video.Title, video.videoPublishedAt FROM channel JOIN playlist ON channel.Channel_Id = playlist.Channel_Id JOIN video ON playlist.Playlist_Id = video.Playlist_Id  WHERE EXTRACT(YEAR FROM videoPublishedAt) = 2022;")
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
            cursor.execute("SELECT channel.Channel_Name, video.title, video.comment_count FROM channel JOIN playlist ON channel.Channel_Id = playlist.Channel_Id JOIN video ON playlist.Playlist_Id = video.Playlist_Id ORDER BY video.comment_count DESC;")
            result_10= cursor.fetchall()
            df10 = pd.DataFrame(result_10,columns=['Channel_Name','title', 'commentcount']).reset_index(drop=True)
            df10.index += 1
            st.dataframe(df10)

        # Q11
        elif question_tosql == '11. What are the names of all the channels that have published videos in the year 2024?':
            cursor.execute("SELECT channel.Channel_Name, video.Title, video.videoPublishedAt FROM channel JOIN playlist ON channel.Channel_Id = playlist.Channel_Id JOIN video ON playlist.Playlist_Id = video.Playlist_Id  WHERE EXTRACT(YEAR FROM videoPublishedAt) = 2024;")
            result_11= cursor.fetchall()
            df11 = pd.DataFrame(result_11,columns=['Channel_Name','title', 'Year 2024 only']).reset_index(drop=True)
            df11.index += 1
            st.dataframe(df11)

        # SQL DB connection close
        connect_for_question.close()
    with col2:
        st_lottie(data, reverse=True, height=400, width=500, speed=1.5, loop=True, quality='high', key='utubeani')
        st.markdown(" ")
        st.markdown(" ")
        st.markdown(" ")
