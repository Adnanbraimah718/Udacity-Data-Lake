## **Project 4: Data Lake**
-----------------------------------------------------------------------------------------------------------------------------------

The objective of this project is to create an ETL pipeline for a Data Lake hosted on Udacity's S3 bucket, s3://udacity-dend/. We are to load the data from S3, process the data into their respective fact and dimension tables using Spark, and then load the parquet files back into S3.


The data contained in S3 bucket are JSON files. 



## **Steps**
-----------------------------------------------------------------------------------------------------------------------------------

I used jupyter notebooks to build a prototype ETL pipeline before adding the code to etl.py for output visualization and debugging purposes. 

Created a star schema detailed in the section below.

A new user was created on AWS IAM with S3 full access. These credentials were then added to dl.cfg


## **Schema**
-----------------------------------------------------------------------------------------------------------------------------------

The schema will consist of four dimension tables and a fact table. The four dimension tables will be the users, songs, artists and time tables. The fact table will be the song plays table. 


### Schema for Song Play Analysis
Using the song and log datasets, you'll need to create a star schema optimized for queries on song play analysis. This includes the following tables. The columns and data for each table is listed below: 

### Fact Table
songplays - records in log data associated with song plays i.e. records with page NextSong
songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

### Dimension Tables

users - users in the app
user_id, first_name, last_name, gender, level

songs - songs in music database
song_id, title, artist_id, year, duration

artists - artists in music database
artist_id, name, location, latitude, longitude

time - timestamps of records in songplays broken down into specific units
start_time, hour, day, week, month, year, weekday


## **Files**
-----------------------------------------------------------------------------------------------------------------------------------

six files included:

dl.cfg which contains AWS credentials such as username and password

Spark_Scratch_File.ipynb which contains my ETL pipeline used for visualization and debugging purposes.

etl.py ETL pipeline used to retrieve data from Udacity Data Lake S3 bucket and load them using PySpark to create fact and dimension tables to store back into my S3 bucket.



Data stored in udacity Data Lake S3 bucket contains JSON format song data and log data. The song data contains data about a song and its artist. The log data is data generated from an event simulator.


## **Installation**
-----------------------------------------------------------------------------------------------------------------------------------


1. Enter Credentials in dl.cfg file
2. Using a new terminal window the etl.py  file should be ran first using the following commands
`python etl.py`

*Notes:

Do not leave created S3 bucket running for long time. Charges can incur. 

