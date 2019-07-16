from datetime import datetime
import sqlite3
import os
import boto3
import configparser
import logging
from shutil import copyfileobj
import bz2
import subprocess


# structure of the tweet that will be used by orc-tools
STRUCTURE_TWEET = "\"struct<created_at:timestamp,id:bigint,id_str:string,text:string,source:string,truncated:boolean,in_reply_to_status_id:bigint,in_reply_to_status_id_str:string,in_reply_to_user_id:bigint,in_reply_to_user_id_str:string,in_reply_to_screen_name:string,quoted_status_id:bigint,quoted_status_id_str:string,is_quote_status:boolean,retweet_count:int,favorite_count:int,favorited:boolean,retweeted:boolean,possibly_sensitive:boolean,filter_level:string,lang:string,user:struct<id:bigint,id_str:string,name:string,screen_name:string,location:string,url:string,description:string,protected:boolean,verified:boolean,followers_count:int,friends_count:int,listed_count:int,favourites_count:int,statuses_count:int,created_at:timestamp,profile_banner_url:string,profile_image_url_https:string,default_profile:boolean,default_profile_image:boolean,withheld_in_countries:array<string>,withheld_scope:string>,coordinates:struct<coordinates:array<float>,type:string>,place:struct<id:string,url:string,place_type:string,name:string,full_name:string,country_code:string,country:string,bounding_box:struct<coordinates:array<array<array<float>>>,type:string>>,entities:struct<hashtags:array<struct<indices:array<smallint>,text:string>>,urls:array<struct<display_url:string,expanded_url:string,indices:array<smallint>,url:string>>,user_mentions:array<struct<id:bigint,id_str:string,indices:array<smallint>,name:string,screen_name:string>>,symbols:array<struct<indices:array<smallint>,text:string>>,media:array<struct<display_url:string,expanded_url:string,id:bigint,id_str:string,indices:array<smallint>,media_url:string,media_url_https:string,source_status_id:bigint,source_status_id_str:string,type:string,url:string>>>,quoted_status:struct<created_at:timestamp,id:bigint,id_str:string,text:string,source:string,truncated:boolean,in_reply_to_status_id:bigint,in_reply_to_status_id_str:string,in_reply_to_user_id:bigint,in_reply_to_user_id_str:string,in_reply_to_screen_name:string,quoted_status_id:bigint,quoted_status_id_str:string,is_quote_status:boolean,retweet_count:int,favorite_count:int,favorited:boolean,retweeted:boolean,possibly_sensitive:boolean,filter_level:string,lang:string,user:struct<id:bigint,id_str:string,name:string,screen_name:string,location:string,url:string,description:string,protected:boolean,verified:boolean,followers_count:int,friends_count:int,listed_count:int,favourites_count:int,statuses_count:int,created_at:timestamp,profile_banner_url:string,profile_image_url_https:string,default_profile:boolean,default_profile_image:boolean,withheld_in_countries:array<string>,withheld_scope:string>,coordinates:struct<coordinates:array<float>,type:string>,place:struct<id:string,url:string,place_type:string,name:string,full_name:string,country_code:string,country:string,bounding_box:struct<coordinates:array<array<array<float>>>,type:string>>,entities:struct<hashtags:array<struct<indices:array<smallint>,text:string>>,urls:array<struct<display_url:string,expanded_url:string,indices:array<smallint>,url:string>>,user_mentions:array<struct<id:bigint,id_str:string,indices:array<smallint>,name:string,screen_name:string>>,symbols:array<struct<indices:array<smallint>,text:string>>,media:array<struct<display_url:string,expanded_url:string,id:bigint,id_str:string,indices:array<smallint>,media_url:string,media_url_https:string,source_status_id:bigint,source_status_id_str:string,type:string,url:string>>>>,retweeted_status:struct<created_at:timestamp,id:bigint,id_str:string,text:string,source:string,truncated:boolean,in_reply_to_status_id:bigint,in_reply_to_status_id_str:string,in_reply_to_user_id:bigint,in_reply_to_user_id_str:string,in_reply_to_screen_name:string,quoted_status_id:bigint,quoted_status_id_str:string,is_quote_status:boolean,retweet_count:int,favorite_count:int,favorited:boolean,retweeted:boolean,possibly_sensitive:boolean,filter_level:string,lang:string,user:struct<id:bigint,id_str:string,name:string,screen_name:string,location:string,url:string,description:string,protected:boolean,verified:boolean,followers_count:int,friends_count:int,listed_count:int,favourites_count:int,statuses_count:int,created_at:timestamp,profile_banner_url:string,profile_image_url_https:string,default_profile:boolean,default_profile_image:boolean,withheld_in_countries:array<string>,withheld_scope:string>,coordinates:struct<coordinates:array<float>,type:string>,place:struct<id:string,url:string,place_type:string,name:string,full_name:string,country_code:string,country:string,bounding_box:struct<coordinates:array<array<array<float>>>,type:string>>,entities:struct<hashtags:array<struct<indices:array<smallint>,text:string>>,urls:array<struct<display_url:string,expanded_url:string,indices:array<smallint>,url:string>>,user_mentions:array<struct<id:bigint,id_str:string,indices:array<smallint>,name:string,screen_name:string>>,symbols:array<struct<indices:array<smallint>,text:string>>,media:array<struct<display_url:string,expanded_url:string,id:bigint,id_str:string,indices:array<smallint>,media_url:string,media_url_https:string,source_status_id:bigint,source_status_id_str:string,type:string,url:string>>>>>\""


# TODO: write README
def main():
    # Configure logging module to save on log file and present messages on the screen too
    log_directory = os.path.join(os.path.dirname(__file__), 'logs')
    os.makedirs(log_directory, exist_ok=True)
    logging.basicConfig(filename=os.path.join(log_directory, 'upload.log'),
                        filemode='w',
                        format='%(asctime)s - %(levelname)s - %(message)s',
                        level=logging.INFO)
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console.setFormatter(formatter)
    logging.getLogger().addHandler(console)
    
    # Specifies the folder for the database
    database_dir = os.path.join(os.path.dirname(__file__), 'db')

    # if the number of days since Jan 1, 1970 is an even number, sends the data on database "odd" to S3
    # (since database "odd" is probably idle now)
    if int(datetime.utcnow().timestamp() / 86400) % 2 == 0:
        logging.info('Going to process odd.sqlite...')
        connection = sqlite3.connect(os.path.join(database_dir, 'odd.sqlite'), isolation_level=None)
    # otherwise, send the data on database "even" to S3
    else:
        logging.info('Going to process even.sqlite...')
        connection = sqlite3.connect(os.path.join(database_dir, 'even.sqlite'), isolation_level=None)
    # Nice dict format for rows on Sqlite database
    connection.row_factory = sqlite3.Row

    # created an index to make the followingoperations faster
    logging.info('Going to create index...')
    connection.execute('create index if not exists idx_tweet on tweet(project, creation_date, tweet_id)')

    # makes config.ini available
    logging.info('Going to read config.ini...')
    config = configparser.ConfigParser()
    config.read(os.path.join(os.path.dirname(__file__), 'config.ini'))

    # connects to S3
    logging.info('Going to connect to S3...')
    session = boto3.Session(
        aws_access_key_id=config['aws']['aws_access_key_id'],
        aws_secret_access_key=config['aws']['aws_secret_access_key'],
        region_name=config['aws']['region']
    )
    s3 = session.resource('s3')
    
    # created temp_dir if does not exist
    logging.info('Going to create S3 directory...')
    temp_directory = os.path.join(os.path.dirname(__file__), 'temp')
    os.makedirs(temp_directory, exist_ok=True)
    
    # specifies the different temporary filenames
    filename_json = os.path.join(temp_directory, 'twitter_stream.json')
    filename_orc = os.path.join(temp_directory, 'twitter_stream.orc')
    filename_bz2 = os.path.join(temp_directory, 'twitter_stream.json.bz2')

    # we are going to need two cursor: one to iterate through the different projects/creation_dates and
    # the other to iterate through each tweet in a given project/creation_date
    cursor_files = connection.cursor()
    cursor_records = connection.cursor()

    logging.info('Going to execute outer query...')
    # num_records will be used in the filename of the data file on S3
    cursor_files.execute('select project, creation_date, count(*) as num_records '
                         'from tweet '
                         'group by project, creation_date '
                         'order by project, creation_date')
    # each pair project-creation_date will be a different file (and in fact a partition) on Athena
    for file in cursor_files:
        logging.info('Processing data for project %s and creation date %s...', file['project'], file['creation_date'])
        logging.info('Going to create file %s...', filename_json)
        # creates a file with all the tweets for a project-creation_date
        with open(filename_json, 'w') as json_file:
            logging.info('Going to execute inner query...')
            cursor_records.execute('select tweet_id, tweet_json '
                                   'from tweet '
                                   'where project = ? and creation_date = ? '
                                   'order by tweet_id', (file['project'], file['creation_date']))
            logging.info('Going to import data from sqlite to json file...')
            for record in cursor_records:
                json_file.write("{}\n".format(record['tweet_json']))

        # compresses the JSON file before sending it to S3
        logging.info('Going to compress JSON file...')
        with open(filename_json, 'rb') as input_file:
            with bz2.BZ2File(filename_bz2, 'wb', compresslevel=9) as output_file:
                copyfileobj(input_file, output_file)

        # upload compressed file to S3
        filename_s3 = 'twitter_stream/project={}/creation_date={}/{}.json.bz2'.format(
            file['project'], file['creation_date'], str(file['num_records']))
        logging.info('Going to upload bz2 file to bucket %s as %s...', config['aws']['s3_bucket_raw'], filename_s3)
        s3.Bucket(config['aws']['s3_bucket_raw']).upload_file(filename_bz2, filename_s3)
        logging.info('Going to delete temporary bz2 file...')
        # remove compressed file
        os.remove(filename_bz2)

        logging.info('Going to convert into ORC format...')
        # if there is already an ORC file, delete it. Otherwise orc-tools will issue an error message
        try:
            os.remove(filename_orc)
        except OSError:
            pass
        # run orc_tools to convert JSON on ORC
        subprocess.run(['java',
                        '-jar', os.path.join(os.path.dirname(__file__), 'utils/orc-tools-1.5.6-uber.jar'),
                        'convert', filename_json,
                        '-o', filename_orc,
                        '-s', STRUCTURE_TWEET],
                       check=True)
        
        # deletes JSON file now that it is not necessary anymore
        logging.info('Going to delete temporary JSON file...')
        os.remove(filename_json)
        
        # uploads ORC file to the right partiion on S3
        filename_s3 = 'twitter_stream/project={}/creation_date={}/{}.orc'.format(
            file['project'], file['creation_date'], str(file['num_records']))
        logging.info('Going to upload ORC file to bucket %s as %s...', config['aws']['s3_bucket'], filename_s3)
        s3.Bucket(config['aws']['s3_bucket']).upload_file(filename_orc, filename_s3)
        logging.info('Going to delete temporary ORC file...')
        
        # deletes local ORC file
        os.remove(filename_orc)

    # empties the table whose data has just been exported to S3
    logging.info('Going to drop table tweet...')
    connection.execute('drop table tweet')
    logging.info('Going to recreate table tweet...')
    connection.execute("""
        create table if not exists tweet
          (project string, creation_date timestamp,
          tweet_id string, tweet_json string)
    """)
    logging.info('Going to vacuum sqlite file...')
    connection.execute("vacuum")
    connection.close()

    # queue commands on athena to delete twitter_stream tables (both JSON and ORC-based) and recreate them
    # with the right partitions
    athena = session.client('athena', region_name=config['aws']['region'])

    logging.info('Going to create Athena database...')
    athena.start_query_execution(
        QueryString="create database if not exists internetscholar".replace('internetscholar',
                                                                            config['aws']['athena_database']),
        ResultConfiguration={'OutputLocation': 's3://{}/twitter_stream'.format(config['aws']['s3_bucket_temp'])}
    )

    logging.info('Going to delete athena tables for twitter_stream...')
    athena.start_query_execution(
        QueryString="drop table if exists twitter_stream",
        QueryExecutionContext={'Database': config['aws']['athena_database']},
        ResultConfiguration={'OutputLocation': 's3://{}/twitter_stream'.format(config['aws']['s3_bucket_temp'])}
    )
    athena.start_query_execution(
        QueryString="drop table if exists twitter_stream_raw",
        QueryExecutionContext={'Database': config['aws']['athena_database']},
        ResultConfiguration={'OutputLocation': 's3://{}/twitter_stream'.format(config['aws']['s3_bucket_temp'])}
    )

    logging.info('Going to recreate athena tables for twitter_stream...')
    athena.start_query_execution(
        QueryString="""
            CREATE EXTERNAL TABLE IF NOT EXISTS twitter_stream (
              `created_at` timestamp,
              `id` bigint,
              `id_str` string,
              `text` string,
              `source` string,
              `truncated` boolean,
              `in_reply_to_status_id` bigint,
              `in_reply_to_status_id_str` string,
              `in_reply_to_user_id` bigint,
              `in_reply_to_user_id_str` string,
              `in_reply_to_screen_name` string,
              `quoted_status_id` bigint,
              `quoted_status_id_str` string,
              `is_quote_status` boolean,
              `retweet_count` int,
              `favorite_count` int,
              `favorited` boolean,
              `retweeted` boolean,
              `possibly_sensitive` boolean,
              `filter_level` string,
              `lang` string,
              `user` struct<`id`: bigint,
                            `id_str`: string,
                            `name`: string,
                            `screen_name`: string,
                            `location`: string,
                            `url`: string,
                            `description`: string,
                            `protected`: boolean,
                            `verified`: boolean,
                            `followers_count`: int,
                            `friends_count`: int,
                            `listed_count`: int,
                            `favourites_count`: int,
                            `statuses_count`: int,
                            `created_at`: timestamp,
                            `profile_banner_url`: string,
                            `profile_image_url_https`: string,
                            `default_profile`: boolean,
                            `default_profile_image`: boolean,
                            `withheld_in_countries`: array<string>,
                            `withheld_scope`: string
                            >,
              `coordinates` struct<`coordinates`: array<float>,
                                   `type`: string
                                   >,
              `place` struct<`id`: string,
                             `url`: string,
                             `place_type`: string,
                             `name`: string,
                             `full_name`: string,
                             `country_code`: string,
                             `country`: string,
                             `bounding_box`: struct<`coordinates`: array<array<array<float>>>,
                                                    `type`: string>
                            >,
              `entities` struct<`hashtags`: array<struct<`indices`: array<smallint>,
                                                         `text`: string
                                                         >
                                                 >,
                                `urls`: array<struct<`display_url`: string,
                                                     `expanded_url`: string,
                                                     `indices`: array<smallint>,
                                                     `url`: string>
                                             >,
                                `user_mentions`: array<struct<`id`: bigint,
                                                              `id_str`: string,
                                                              `indices`: array<smallint>,
                                                              `name`: string,
                                                              `screen_name`: string
                                                             >
                                                      >,
                                `symbols`: array<struct<`indices`: array<smallint>,
                                                         `text`: string
                                                         >
                                                 >,
                                `media`: array<struct<`display_url`: string,
                                                      `expanded_url`: string,
                                                      `id`: bigint,
                                                      `id_str`: string,
                                                      `indices`: array<smallint>,
                                                      `media_url`: string,
                                                      `media_url_https`: string,
                                                      `source_status_id`: bigint,
                                                      `source_status_id_str`: string,
                                                      `type`: string,
                                                      `url`: string
                                                    >
                                             >
                                >,
              `quoted_status` struct<`created_at`: timestamp,
                                      `id`: bigint,
                                      `id_str`: string,
                                      `text`: string,
                                      `source`: string,
                                      `truncated`: boolean,
                                      `in_reply_to_status_id`: bigint,
                                      `in_reply_to_status_id_str`: string,
                                      `in_reply_to_user_id`: bigint,
                                      `in_reply_to_user_id_str`: string,
                                      `in_reply_to_screen_name`: string,
                                      `quoted_status_id`: bigint,
                                      `quoted_status_id_str`: string,
                                      `is_quote_status`: boolean,
                                      `retweet_count`: int,
                                      `favorite_count`: int,
                                      `favorited`: boolean,
                                      `retweeted`: boolean,
                                      `possibly_sensitive`: boolean,
                                      `filter_level`: string,
                                      `lang`: string,
                                      `user`: struct<`id`: bigint,
                                                    `id_str`: string,
                                                    `name`: string,
                                                    `screen_name`: string,
                                                    `location`: string,
                                                    `url`: string,
                                                    `description`: string,
                                                    `protected`: boolean,
                                                    `verified`: boolean,
                                                    `followers_count`: int,
                                                    `friends_count`: int,
                                                    `listed_count`: int,
                                                    `favourites_count`: int,
                                                    `statuses_count`: int,
                                                    `created_at`: timestamp,
                                                    `profile_banner_url`: string,
                                                    `profile_image_url_https`: string,
                                                    `default_profile`: boolean,
                                                    `default_profile_image`: boolean,
                                                    `withheld_in_countries`: array<string>,
                                                    `withheld_scope`: string
                                                    >,
                                      `coordinates`: struct<`coordinates`: array<float>,
                                                           `type`: string
                                                           >,
                                      `place`: struct<`id`: string,
                                                     `url`: string,
                                                     `place_type`: string,
                                                     `name`: string,
                                                     `full_name`: string,
                                                     `country_code`: string,
                                                     `country`: string,
                                                     `bounding_box`: struct<`coordinates`: array<array<array<float>>>,
                                                                            `type`: string>
                                                    >,
                                      `entities`:struct<`hashtags`: array<struct<`indices`: array<smallint>,
                                                                                 `text`: string
                                                                                 >
                                                                         >,
                                                        `urls`: array<struct<`display_url`: string,
                                                                             `expanded_url`: string,
                                                                             `indices`: array<smallint>,
                                                                             `url`: string>
                                                                     >,
                                                        `user_mentions`: array<struct<`id`: bigint,
                                                                                      `id_str`: string,
                                                                                      `indices`: array<smallint>,
                                                                                      `name`: string,
                                                                                      `screen_name`: string
                                                                                     >
                                                                              >,
                                                        `symbols`: array<struct<`indices`: array<smallint>,
                                                                                 `text`: string
                                                                                 >
                                                                         >,
                                                        `media`: array<struct<`display_url`: string,
                                                                              `expanded_url`: string,
                                                                              `id`: bigint,
                                                                              `id_str`: string,
                                                                              `indices`: array<smallint>,
                                                                              `media_url`: string,
                                                                              `media_url_https`: string,
                                                                              `source_status_id`: bigint,
                                                                              `source_status_id_str`: string,
                                                                              `type`: string,
                                                                              `url`: string
                                                                            >
                                                                     >
                                                        >
                                      >,
              `retweeted_status` struct<`created_at`: timestamp,
                                      `id`: bigint,
                                      `id_str`: string,
                                      `text`: string,
                                      `source`: string,
                                      `truncated`: boolean,
                                      `in_reply_to_status_id`: bigint,
                                      `in_reply_to_status_id_str`: string,
                                      `in_reply_to_user_id`: bigint,
                                      `in_reply_to_user_id_str`: string,
                                      `in_reply_to_screen_name`: string,
                                      `quoted_status_id`: bigint,
                                      `quoted_status_id_str`: string,
                                      `is_quote_status`: boolean,
                                      `retweet_count`: int,
                                      `favorite_count`: int,
                                      `favorited`: boolean,
                                      `retweeted`: boolean,
                                      `possibly_sensitive`: boolean,
                                      `filter_level`: string,
                                      `lang`: string,
                                      `user`: struct<`id`: bigint,
                                                    `id_str`: string,
                                                    `name`: string,
                                                    `screen_name`: string,
                                                    `location`: string,
                                                    `url`: string,
                                                    `description`: string,
                                                    `protected`: boolean,
                                                    `verified`: boolean,
                                                    `followers_count`: int,
                                                    `friends_count`: int,
                                                    `listed_count`: int,
                                                    `favourites_count`: int,
                                                    `statuses_count`: int,
                                                    `created_at`: timestamp,
                                                    `profile_banner_url`: string,
                                                    `profile_image_url_https`: string,
                                                    `default_profile`: boolean,
                                                    `default_profile_image`: boolean,
                                                    `withheld_in_countries`: array<string>,
                                                    `withheld_scope`: string
                                                    >,
                                      `coordinates`: struct<`coordinates`: array<float>,
                                                           `type`: string
                                                           >,
                                      `place`: struct<`id`: string,
                                                     `url`: string,
                                                     `place_type`: string,
                                                     `name`: string,
                                                     `full_name`: string,
                                                     `country_code`: string,
                                                     `country`: string,
                                                     `bounding_box`: struct<`coordinates`: array<array<array<float>>>,
                                                                            `type`: string>
                                                    >,
                                      `entities`:struct<`hashtags`: array<struct<`indices`: array<smallint>,
                                                                                 `text`: string
                                                                                 >
                                                                         >,
                                                        `urls`: array<struct<`display_url`: string,
                                                                             `expanded_url`: string,
                                                                             `indices`: array<smallint>,
                                                                             `url`: string>
                                                                     >,
                                                        `user_mentions`: array<struct<`id`: bigint,
                                                                                      `id_str`: string,
                                                                                      `indices`: array<smallint>,
                                                                                      `name`: string,
                                                                                      `screen_name`: string
                                                                                     >
                                                                              >,
                                                        `symbols`: array<struct<`indices`: array<smallint>,
                                                                                 `text`: string
                                                                                 >
                                                                         >,
                                                        `media`: array<struct<`display_url`: string,
                                                                              `expanded_url`: string,
                                                                              `id`: bigint,
                                                                              `id_str`: string,
                                                                              `indices`: array<smallint>,
                                                                              `media_url`: string,
                                                                              `media_url_https`: string,
                                                                              `source_status_id`: bigint,
                                                                              `source_status_id_str`: string,
                                                                              `type`: string,
                                                                              `url`: string
                                                                            >
                                                                     >
                                                        >
                                      >
            )
            PARTITIONED BY (project String, creation_date String)
            STORED AS ORC
            LOCATION 's3://internetscholar/twitter_stream/'
            tblproperties ("orc.compress"="ZLIB");
        """.replace('s3://internetscholar/twitter_stream/',
                    "s3://{}/twitter_stream/".format(config['aws']['s3_bucket'])),
        QueryExecutionContext={'Database': config['aws']['athena_database']},
        ResultConfiguration={'OutputLocation': 's3://{}/twitter_stream'.format(config['aws']['s3_bucket_temp'])}
    )
    athena.start_query_execution(
        QueryString="""
            CREATE EXTERNAL TABLE IF NOT EXISTS twitter_stream_raw (
              `created_at` timestamp,
              `id` bigint,
              `id_str` string,
              `text` string,
              `source` string,
              `truncated` boolean,
              `in_reply_to_status_id` bigint,
              `in_reply_to_status_id_str` string,
              `in_reply_to_user_id` bigint,
              `in_reply_to_user_id_str` string,
              `in_reply_to_screen_name` string,
              `quoted_status_id` bigint,
              `quoted_status_id_str` string,
              `is_quote_status` boolean,
              `retweet_count` int,
              `favorite_count` int,
              `favorited` boolean,
              `retweeted` boolean,
              `possibly_sensitive` boolean,
              `filter_level` string,
              `lang` string,
              `user` struct<`id`: bigint,
                            `id_str`: string,
                            `name`: string,
                            `screen_name`: string,
                            `location`: string,
                            `url`: string,
                            `description`: string,
                            `protected`: boolean,
                            `verified`: boolean,
                            `followers_count`: int,
                            `friends_count`: int,
                            `listed_count`: int,
                            `favourites_count`: int,
                            `statuses_count`: int,
                            `created_at`: timestamp,
                            `profile_banner_url`: string,
                            `profile_image_url_https`: string,
                            `default_profile`: boolean,
                            `default_profile_image`: boolean,
                            `withheld_in_countries`: array<string>,
                            `withheld_scope`: string
                            >,
              `coordinates` struct<`coordinates`: array<float>,
                                   `type`: string
                                   >,
              `place` struct<`id`: string,
                             `url`: string,
                             `place_type`: string,
                             `name`: string,
                             `full_name`: string,
                             `country_code`: string,
                             `country`: string,
                             `bounding_box`: struct<`coordinates`: array<array<array<float>>>,
                                                    `type`: string>
                            >,
              `entities` struct<`hashtags`: array<struct<`indices`: array<smallint>,
                                                         `text`: string
                                                         >
                                                 >,
                                `urls`: array<struct<`display_url`: string,
                                                     `expanded_url`: string,
                                                     `indices`: array<smallint>,
                                                     `url`: string>
                                             >,
                                `user_mentions`: array<struct<`id`: bigint,
                                                              `id_str`: string,
                                                              `indices`: array<smallint>,
                                                              `name`: string,
                                                              `screen_name`: string
                                                             >
                                                      >,
                                `symbols`: array<struct<`indices`: array<smallint>,
                                                         `text`: string
                                                         >
                                                 >,
                                `media`: array<struct<`display_url`: string,
                                                      `expanded_url`: string,
                                                      `id`: bigint,
                                                      `id_str`: string,
                                                      `indices`: array<smallint>,
                                                      `media_url`: string,
                                                      `media_url_https`: string,
                                                      `source_status_id`: bigint,
                                                      `source_status_id_str`: string,
                                                      `type`: string,
                                                      `url`: string
                                                    >
                                             >
                                >,
              `quoted_status` struct<`created_at`: timestamp,
                                      `id`: bigint,
                                      `id_str`: string,
                                      `text`: string,
                                      `source`: string,
                                      `truncated`: boolean,
                                      `in_reply_to_status_id`: bigint,
                                      `in_reply_to_status_id_str`: string,
                                      `in_reply_to_user_id`: bigint,
                                      `in_reply_to_user_id_str`: string,
                                      `in_reply_to_screen_name`: string,
                                      `quoted_status_id`: bigint,
                                      `quoted_status_id_str`: string,
                                      `is_quote_status`: boolean,
                                      `retweet_count`: int,
                                      `favorite_count`: int,
                                      `favorited`: boolean,
                                      `retweeted`: boolean,
                                      `possibly_sensitive`: boolean,
                                      `filter_level`: string,
                                      `lang`: string,
                                      `user`: struct<`id`: bigint,
                                                    `id_str`: string,
                                                    `name`: string,
                                                    `screen_name`: string,
                                                    `location`: string,
                                                    `url`: string,
                                                    `description`: string,
                                                    `protected`: boolean,
                                                    `verified`: boolean,
                                                    `followers_count`: int,
                                                    `friends_count`: int,
                                                    `listed_count`: int,
                                                    `favourites_count`: int,
                                                    `statuses_count`: int,
                                                    `created_at`: timestamp,
                                                    `profile_banner_url`: string,
                                                    `profile_image_url_https`: string,
                                                    `default_profile`: boolean,
                                                    `default_profile_image`: boolean,
                                                    `withheld_in_countries`: array<string>,
                                                    `withheld_scope`: string
                                                    >,
                                      `coordinates`: struct<`coordinates`: array<float>,
                                                           `type`: string
                                                           >,
                                      `place`: struct<`id`: string,
                                                     `url`: string,
                                                     `place_type`: string,
                                                     `name`: string,
                                                     `full_name`: string,
                                                     `country_code`: string,
                                                     `country`: string,
                                                     `bounding_box`: struct<`coordinates`: array<array<array<float>>>,
                                                                            `type`: string>
                                                    >,
                                      `entities`:struct<`hashtags`: array<struct<`indices`: array<smallint>,
                                                                                 `text`: string
                                                                                 >
                                                                         >,
                                                        `urls`: array<struct<`display_url`: string,
                                                                             `expanded_url`: string,
                                                                             `indices`: array<smallint>,
                                                                             `url`: string>
                                                                     >,
                                                        `user_mentions`: array<struct<`id`: bigint,
                                                                                      `id_str`: string,
                                                                                      `indices`: array<smallint>,
                                                                                      `name`: string,
                                                                                      `screen_name`: string
                                                                                     >
                                                                              >,
                                                        `symbols`: array<struct<`indices`: array<smallint>,
                                                                                 `text`: string
                                                                                 >
                                                                         >,
                                                        `media`: array<struct<`display_url`: string,
                                                                              `expanded_url`: string,
                                                                              `id`: bigint,
                                                                              `id_str`: string,
                                                                              `indices`: array<smallint>,
                                                                              `media_url`: string,
                                                                              `media_url_https`: string,
                                                                              `source_status_id`: bigint,
                                                                              `source_status_id_str`: string,
                                                                              `type`: string,
                                                                              `url`: string
                                                                            >
                                                                     >
                                                        >
                                      >,
              `retweeted_status` struct<`created_at`: timestamp,
                                      `id`: bigint,
                                      `id_str`: string,
                                      `text`: string,
                                      `source`: string,
                                      `truncated`: boolean,
                                      `in_reply_to_status_id`: bigint,
                                      `in_reply_to_status_id_str`: string,
                                      `in_reply_to_user_id`: bigint,
                                      `in_reply_to_user_id_str`: string,
                                      `in_reply_to_screen_name`: string,
                                      `quoted_status_id`: bigint,
                                      `quoted_status_id_str`: string,
                                      `is_quote_status`: boolean,
                                      `retweet_count`: int,
                                      `favorite_count`: int,
                                      `favorited`: boolean,
                                      `retweeted`: boolean,
                                      `possibly_sensitive`: boolean,
                                      `filter_level`: string,
                                      `lang`: string,
                                      `user`: struct<`id`: bigint,
                                                    `id_str`: string,
                                                    `name`: string,
                                                    `screen_name`: string,
                                                    `location`: string,
                                                    `url`: string,
                                                    `description`: string,
                                                    `protected`: boolean,
                                                    `verified`: boolean,
                                                    `followers_count`: int,
                                                    `friends_count`: int,
                                                    `listed_count`: int,
                                                    `favourites_count`: int,
                                                    `statuses_count`: int,
                                                    `created_at`: timestamp,
                                                    `profile_banner_url`: string,
                                                    `profile_image_url_https`: string,
                                                    `default_profile`: boolean,
                                                    `default_profile_image`: boolean,
                                                    `withheld_in_countries`: array<string>,
                                                    `withheld_scope`: string
                                                    >,
                                      `coordinates`: struct<`coordinates`: array<float>,
                                                           `type`: string
                                                           >,
                                      `place`: struct<`id`: string,
                                                     `url`: string,
                                                     `place_type`: string,
                                                     `name`: string,
                                                     `full_name`: string,
                                                     `country_code`: string,
                                                     `country`: string,
                                                     `bounding_box`: struct<`coordinates`: array<array<array<float>>>,
                                                                            `type`: string>
                                                    >,
                                      `entities`:struct<`hashtags`: array<struct<`indices`: array<smallint>,
                                                                                 `text`: string
                                                                                 >
                                                                         >,
                                                        `urls`: array<struct<`display_url`: string,
                                                                             `expanded_url`: string,
                                                                             `indices`: array<smallint>,
                                                                             `url`: string>
                                                                     >,
                                                        `user_mentions`: array<struct<`id`: bigint,
                                                                                      `id_str`: string,
                                                                                      `indices`: array<smallint>,
                                                                                      `name`: string,
                                                                                      `screen_name`: string
                                                                                     >
                                                                              >,
                                                        `symbols`: array<struct<`indices`: array<smallint>,
                                                                                 `text`: string
                                                                                 >
                                                                         >,
                                                        `media`: array<struct<`display_url`: string,
                                                                              `expanded_url`: string,
                                                                              `id`: bigint,
                                                                              `id_str`: string,
                                                                              `indices`: array<smallint>,
                                                                              `media_url`: string,
                                                                              `media_url_https`: string,
                                                                              `source_status_id`: bigint,
                                                                              `source_status_id_str`: string,
                                                                              `type`: string,
                                                                              `url`: string
                                                                            >
                                                                     >
                                                        >
                                      >
            )
            PARTITIONED BY (project String, creation_date String)
            ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
            WITH SERDEPROPERTIES (
              'serialization.format' = '1',
              'ignore.malformed.json' = 'true'
            ) LOCATION 's3://internetscholar-raw/twitter_stream/'
            TBLPROPERTIES ('has_encrypted_data'='false');
        """.replace('s3://internetscholar-raw/twitter_stream/',
                    "s3://{}/twitter_stream/".format(config['aws']['s3_bucket_raw'])),
        QueryExecutionContext={'Database': config['aws']['athena_database']},
        ResultConfiguration={'OutputLocation': 's3://{}/twitter_stream'.format(config['aws']['s3_bucket_temp'])}
    )

    logging.info('Going to repair partitions for twitter_stream...')
    athena.start_query_execution(
        QueryString="MSCK REPAIR TABLE twitter_stream",
        QueryExecutionContext={'Database': config['aws']['athena_database']},
        ResultConfiguration={'OutputLocation': 's3://{}/twitter_stream'.format(config['aws']['s3_bucket_temp'])}
    )
    athena.start_query_execution(
        QueryString="MSCK REPAIR TABLE twitter_stream_raw",
        QueryExecutionContext={'Database': config['aws']['athena_database']},
        ResultConfiguration={'OutputLocation': 's3://{}/twitter_stream'.format(config['aws']['s3_bucket_temp'])}
    )

    # delete result files on S3 (just a log of the previous commands)
    s3.Bucket(config['aws']['s3_bucket_temp']).objects.filter(Prefix="twitter_stream/").delete()
    logging.info('Deleted result file on S3 for CTAS commands')

    logging.info('End.')


if __name__ == '__main__':
    main()
