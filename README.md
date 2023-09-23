# Sparkify Data Warehouse

This repository is the work for my project 2 from the Udacity Data Engineering with AWS Nanodegree Program. In this project, I will apply what I've learned on data warehouses and AWS to build an ETL pipeline for a database hosted on Redshift.

About Sparkify, a music streaming startup has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

## Objectives

To complete the project, data from S3 is loaded to staging tables on Redshift, and execute SQL statements that create the analytics tables from these staging tables.

## Datasets

### Staging tables

I'll be working with 3 datasets that reside in S3 (region `us-west-2`). These datasets will be loaded to the staging tables.

Here are the S3 links for each:

- Song data: `s3://udacity-dend/song_data`
- Log data: `s3://udacity-dend/log_data`
- This third file `s3://udacity-dend/log_json_path.json` contains the meta information that is required by AWS to correctly load `s3://udacity-dend/log_data`

### Fact table

`songplays`: records in event data associated with song plays i.e. records with page `NextSong`.

- songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

### Dimension tables

1. `users`: users in the app

    - user_id, first_name, last_name, gender, level

2. `songs`: songs in music database

    - song_id, title, artist_id, year, duration

3. `artists`: artists in music database

    - artist_id, name, location, lattitude, longitude

4. `time`: timestamps of records in songplays broken down into specific units

    - start_time, hour, day, week, month, year, weekday

## Instructions

In this project, there are 4 files:

- `create_table.py` is where you'll create your fact and dimension tables for the star schema in Redshift.
- `etl.py` is where you'll load data from S3 into staging tables on Redshift and then process that data into your analytics tables on Redshift.
- `sql_queries.py` is where you'll define you SQL statements, which will be imported into the two other files above.
- `dwh.cfg` is where you'll config the Redshift endpoint and your IAM role.

### How to run the script

1. In `dwh.cfg` file, set the `CLUSTER` variables.
2. Run `python create_tables.py` to drop and create tables for this project.
3. Run `python etl.py` to load datasets in S3 to staging tables and run the ETL process.
