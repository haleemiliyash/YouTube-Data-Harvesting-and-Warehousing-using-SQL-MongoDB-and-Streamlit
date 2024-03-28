#==================================================Import detail=======================================================================#
import googleapiclient.discovery
import pymongo
import mysql.connector
import pandas as pd
import streamlit as st
import isodate
#==================================================SQL connection======================================================================#
mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="",
)
mycursor=mydb.cursor(buffered=True)
#=====================================================MongoDB coneection===============================================================#
client=pymongo.MongoClient("mongodb+srv://abdulhaleem6:74108520@cluster0.fzizifl.mongodb.net/?retryWrites=true&w=majority")
db=client["YouTHarvest_data"]

#======================================================User input=======================================================================#
st.set_page_config(page_title='My Youtube Project')
st.sidebar.header('Youtube data harvesting and Warhousing Using SQL,mongoDB and streamlit')
c_id=st.sidebar.text_input('Enter the channel ID')
#====================================================API connection======================================================================#
API_key="AIzaSyBWVAn4wpqsYPxsjBvyo0jzTr3sLvmYhFU"
api_service_name = "youtube"
api_version = "v3"
youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=API_key)

request= youtube.channels().list(
part="snippet,contentDetails,statistics",
id=c_id
)
response = request.execute()

#======================================================channel details==================================================================#
def get_channel_detail(c_id):
    request= youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=c_id
    )
    response = request.execute()
    for i in response['items']:
        data=dict(channel_name=i['snippet']['title'],
        channel_ID=i['id'],
        c_sub_count=i['statistics']['subscriberCount'],
        c_view_count=i['statistics']['viewCount'],
        channel_description=i['snippet']['description'],
        playlist_ID=i['contentDetails']['relatedPlaylists']['uploads'],
        video_count=i['statistics']['videoCount'])
    return data

#===================================================Video ID=============================================================================#

def get_videos_id(c_id):
    video_ids=[]
    request=youtube.channels().list(part='contentDetails',
                                     id=c_id)
    response=request.execute()
    Playlist_Id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token=None

    while True:
        responseP=youtube.playlistItems().list(
                                            part="snippet,contentDetails",
                                            playlistId=Playlist_Id,
                                            maxResults=50,
                                            pageToken=next_page_token).execute()
        for i in range(len(responseP['items'])):
            video_ids.append(responseP['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token=responseP.get('nextPageToken')

        if next_page_token is None:
            break
    return video_ids

#========================================================video detail==================================================================#

def get_video_detail(video_ids):
    videoinfo=[]
    for x in video_ids:
        request= youtube.videos().list(
        part="snippet,contentDetails,statistics",
        id=x
    )
        response = request.execute()
        for i in response['items']:
            data1=dict(video_ID=i['id'],
                video_title=i['snippet']['title'],
                video_description=i['snippet'].get('description'),
                Year=i['snippet']['publishedAt'][0:4],
                v_published=i['snippet']['publishedAt'],
                V_view_count=i['statistics'].get('viewCount'),
                v_like_count=i['statistics'].get('likeCount'),
                v_favorite_count=i['statistics']['favoriteCount'],
                v_comment_count=i['statistics'].get('commentCount'),
                v_duration=isodate.parse_duration(i['contentDetails']['duration']).total_seconds(),
                C_id=i['snippet']['channelId']
            )
            videoinfo.append(data1)
    return videoinfo

#========================================================comment detail=================================================================#
def get_comment_det(video_ids):
    commentdet=[]
    
    for id in video_ids:
        try:
            request = youtube.commentThreads().list(
                part="snippet",
                videoId=id,
                maxResults=100
            )
            response = request.execute()
            for j in response['items']:
                comment_detail=dict(comment_id=j['id'],
                comment_txt=j['snippet']['topLevelComment']['snippet']['textDisplay'],
                comment_author=j['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                comment_publishat=j['snippet']['topLevelComment']['snippet']['publishedAt'],
                video_id=id
                )
                commentdet.append(comment_detail)
        except:
            pass
    return commentdet

#============================================All data transfer to MongoDB===============================================================
def channel_DT(c_id):
    channel_dt=get_channel_detail(c_id)
    video_id=get_videos_id(c_id)
    video_dt=get_video_detail(video_id)
    comment_info=get_comment_det(video_id)

    data={'channel_details':channel_dt,
          'video_details':video_dt,
          'comment_details':comment_info
          }
    collection=db['channel_dt']
    collection.insert_one({'_id':c_id,'channel_data':channel_dt,'video_data':video_dt,'comment_data':comment_info})
    st.sidebar.write("MongoDB Stored Successfully")
    return data

if c_id and st.sidebar.button("submit"):
    a=channel_DT(c_id)
#==================================================================DATA TRANSFER TO SQL DATABASE==========================================================================================================================================================================================================#
if c_id and st.sidebar.button("Migrate to SQL"):
    try:
        mycursor.execute("CREATE DATABASE youtubeinfo")
        mycursor.execute("USE youtubeinfo")
        mycursor.execute("CREATE TABLE channels(channel_name VARCHAR(255), channel_id VARCHAR(255) PRIMARY KEY, subscribers_count INT,channel_views INT,channel_description TEXT,playlist_ID VARCHAR(255),video_count INT)")
        mycursor.execute("CREATE TABLE videos(video_id VARCHAR(255) PRIMARY KEY,video_title VARCHAR(255),video_description TEXT,Year INT,published_date DATETIME,total_views INT,total_likes INT,favorites_count INT,total_comments INT,duration INT,channel_ID VARCHAR(255))")
        mycursor.execute("CREATE TABLE comments(comment_id VARCHAR(255) primary key,comment_text TEXT,comment_author VARCHAR(200),comment_published DATETIME,video_id VARCHAR(255))")
    except:
        mycursor.execute("USE youtubeinfo")
    try:
        #channal detail
        collection1=db['channel_dt']
        data=collection1.find_one({'channel_data.channel_ID':c_id})
        x=data['channel_data']
        final1=tuple(x.values())
        #print(final1)
        mycursor.execute(f'INSERT INTO channels values{final1}')
        mydb.commit()

        #video detail
        video_list=[]
        collection2=db['channel_dt']
        data2=collection2.find_one({'channel_data.channel_ID':c_id})
        y=data2['video_data']
        for i in y:
            final2=tuple(i.values())
            video_list.append(final2)
            sql = "INSERT INTO videos (video_id,video_title,video_description,Year,published_date,total_views,total_likes,favorites_count,total_comments,duration,channel_ID) VALUES (%s, %s,%s, %s,%s, %s,%s, %s,%s, %s,%s)"
            #print(final1)
            #val=video_list
        mycursor.executemany(sql,video_list)
        mydb.commit()

        #comment detail
        comment_list=[]
        collection3=db['channel_dt']
        data3=collection3.find_one({'channel_data.channel_ID':c_id})
        z=data3['comment_data']
        for j in z:
            final3=tuple(j.values())
            comment_list.append(final3)
            sql="INSERT INTO comments(comment_id,comment_text,comment_author,comment_published,video_id) VALUES (%s, %s,%s, %s,%s)"
            #print(final3)
        mycursor.executemany(sql,comment_list)
        mydb.commit()   
        st.sidebar.write("SQL Migration completed")
    except:
        st.sidebar.write("The data already exists in SQL") 

#=============================================================VISUALIZE TABLE FROM SQL============================================================================================================================#

show=st.sidebar.radio('select any one',('None','show tables','Queries'))
if show=='show tables':
        mycursor.execute("USE youtubeinfo")
        mycursor.execute('SELECT channel_name,channel_id,subscribers_count,channel_views,channel_description,playlist_ID,video_count FROM channels')
        st.subheader("CHANNEL_DETAILS")
        output=mycursor.fetchall()
        st.write(pd.DataFrame(output,columns=['channel_name','channel_id','subscribers_count','channel_views','channel_description','playlist_ID','video_count']))
        
        mycursor.execute('SELECT video_id,video_title,video_description,Year,published_date,total_views,total_likes,favorites_count,total_comments,duration,channel_ID FROM videos')
        st.subheader("VIDEO_DETAILS")
        output=mycursor.fetchall()
        st.write(pd.DataFrame(output,columns=['video_id','video_title','video_description','Year','published_date','total_views','total_likes','favorites_count','total_comments','duration','channel_ID']))
    
        mycursor.execute('SELECT comment_id,comment_text,comment_author,comment_published,video_id FROM comments')
        st.subheader('COMMENT_DETAILS')
        output=mycursor.fetchall()
        st.write(pd.DataFrame(output,columns=['comment_id','comment_text','comment_author','comment_published','video_id']))

#==============================================================QUERIES=============================================================================================================================================#
if show=='Queries':
        st.sidebar.subheader('Queries')
        SelectQuery=st.sidebar.selectbox('Select a Queries',('Query 1','Query 2','Query 3','Query 4','Query 5','Query 6','Query 7','Query 8','Query 9','Query 10'))
        if SelectQuery=='Query 1':
                st.subheader('The name of channel and corresponding videos')
                mycursor.execute('USE youtubeinfo')
                mycursor.execute('SELECT channel_name,video_title FROM channels,videos WHERE channels.channel_id=videos.channel_ID')
                output=mycursor.fetchall()
                #print(output)
                st.write(pd.DataFrame(output,columns=['channel name','video name']))
    
        if SelectQuery=='Query 2':
                st.subheader('Which channels have the most number of videos, and how many videos do they have')
                mycursor.execute("USE youtubeinfo")
                mycursor.execute("SELECT channel_name,video_count FROM channels WHERE video_count IN (SELECT max(video_count) from channels)")
                output=mycursor.fetchall()
                st.write(pd.DataFrame(output,columns=['channel name','total videos']))

        if SelectQuery=='Query 3':
                st.subheader("The top 10 most viewed videos and their respective channels")
                mycursor.execute("USE youtubeinfo")
                mycursor.execute("select video_title,channel_name,rank() over (order by total_views) as rank from videos,channels where channels.channel_id=videos.channel_ID limit 10")
                out=mycursor.fetchall()
                st.write(pd.DataFrame(out,columns=['video name','channel name','rank']))

        if SelectQuery=='Query 4':
                st.subheader('How many comments were made on each video, and what are their corresponding video names')
                mycursor.execute('USE youtubeinfo')
                mycursor.execute('select total_comments,video_title from videos')
                output=mycursor.fetchall()
                st.write(pd.DataFrame(output,columns=['total_comment','video_title']))

        if SelectQuery=='Query 5':
                st.subheader('Which videos have the highest number of likes, and what are their corresponding channel names')
                mycursor.execute('USE youtubeinfo')
                mycursor.execute('SELECT video_title,channel_name,rank() over (ORDER BY total_likes) AS rank FROM videos,channels where channels.channel_id=videos.channel_ID')
                output=mycursor.fetchall()
                st.write(pd.DataFrame(output,columns=['video_title','channel_name','rank']))

        if SelectQuery=='Query 6':
                st.subheader('What is the total number of likes for each video, and what are their corresponding video names')
                mycursor.execute('USE youtubeinfo')
                mycursor.execute('SELECT total_likes,video_title from videos')
                output=mycursor.fetchall()
                st.write(pd.DataFrame(output,columns=['total_like','video_title']))

        if SelectQuery=='Query 7':
                st.subheader('What is the total number of views for each channel, and what are their corresponding channel names')
                mycursor.execute('USE youtubeinfo')
                mycursor.execute('SELECT channel_views,channel_name from channels')
                output=mycursor.fetchall()
                st.write(pd.DataFrame(output,columns=['channel_views','channel_name']))

        if SelectQuery=='Query 8':
                st.subheader("What are the names of all the channels that have published videos in the year 2022")
                mycursor.execute('USE youtubeinfo')
                mycursor.execute("SELECT distinct(channel_name) from channels,videos where Year=2022 and channels.channel_id=videos.channel_ID ")
                out=mycursor.fetchall()
                st.write(pd.DataFrame(out,columns=['channel name']))

        if SelectQuery=='Query 9':
                st.subheader('What is the average duration of all videos in each channel, and what are their corresponding channel names')
                mycursor.execute('USE youtubeinfo')
                mycursor.execute('SELECT (duration)/2,video_title,channel_name FROM videos,channels WHERE channels.channel_id=videos.channel_ID')
                output=mycursor.fetchall()
                st.write(pd.DataFrame(output,columns=['Avg_duration','video_title','channel_name']))

    
        if SelectQuery=='Query 10':
                st.subheader("Which videos have the highest number of comments, and what are their corresponding channel names")
                mycursor.execute('USE youtubeinfo')
                mycursor.execute('SELECT channel_name,video_title,total_comments FROM videos,channels where total_comments in (select max(total_comments) from videos) and channels.channel_id=videos.channel_ID')
                output=mycursor.fetchall()
                st.write(pd.DataFrame(output,columns=['channel_name','video_title','total_comments']))

    