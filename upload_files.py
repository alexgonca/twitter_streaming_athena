from datetime import datetime
import sqlite3
import os
import boto3
import configparser
import logging
from shutil import copyfileobj
import bz2
import subprocess


STRUCTURE_TWEET = "\"struct<created_at:timestamp,id:bigint,id_str:string,text:string,source:string,truncated:boolean,in_reply_to_status_id:bigint,in_reply_to_status_id_str:string,in_reply_to_user_id:bigint,in_reply_to_user_id_str:string,in_reply_to_screen_name:string,quoted_status_id:bigint,quoted_status_id_str:string,is_quote_status:boolean,retweet_count:int,favorite_count:int,favorited:boolean,retweeted:boolean,possibly_sensitive:boolean,filter_level:string,lang:string,user:struct<id:bigint,id_str:string,name:string,screen_name:string,location:string,url:string,description:string,protected:boolean,verified:boolean,followers_count:int,friends_count:int,listed_count:int,favourites_count:int,statuses_count:int,created_at:timestamp,profile_banner_url:string,profile_image_url_https:string,default_profile:boolean,default_profile_image:boolean,withheld_in_countries:array<string>,withheld_scope:string>,coordinates:struct<coordinates:array<float>,type:string>,place:struct<id:string,url:string,place_type:string,name:string,full_name:string,country_code:string,country:string,bounding_box:struct<coordinates:array<array<array<float>>>,type:string>>,entities:struct<hashtags:array<struct<indices:array<smallint>,text:string>>,urls:array<struct<display_url:string,expanded_url:string,indices:array<smallint>,url:string>>,user_mentions:array<struct<id:bigint,id_str:string,indices:array<smallint>,name:string,screen_name:string>>,symbols:array<struct<indices:array<smallint>,text:string>>,media:array<struct<display_url:string,expanded_url:string,id:bigint,id_str:string,indices:array<smallint>,media_url:string,media_url_https:string,source_status_id:bigint,source_status_id_str:string,type:string,url:string>>>,quoted_status:struct<created_at:timestamp,id:bigint,id_str:string,text:string,source:string,truncated:boolean,in_reply_to_status_id:bigint,in_reply_to_status_id_str:string,in_reply_to_user_id:bigint,in_reply_to_user_id_str:string,in_reply_to_screen_name:string,quoted_status_id:bigint,quoted_status_id_str:string,is_quote_status:boolean,retweet_count:int,favorite_count:int,favorited:boolean,retweeted:boolean,possibly_sensitive:boolean,filter_level:string,lang:string,user:struct<id:bigint,id_str:string,name:string,screen_name:string,location:string,url:string,description:string,protected:boolean,verified:boolean,followers_count:int,friends_count:int,listed_count:int,favourites_count:int,statuses_count:int,created_at:timestamp,profile_banner_url:string,profile_image_url_https:string,default_profile:boolean,default_profile_image:boolean,withheld_in_countries:array<string>,withheld_scope:string>,coordinates:struct<coordinates:array<float>,type:string>,place:struct<id:string,url:string,place_type:string,name:string,full_name:string,country_code:string,country:string,bounding_box:struct<coordinates:array<array<array<float>>>,type:string>>,entities:struct<hashtags:array<struct<indices:array<smallint>,text:string>>,urls:array<struct<display_url:string,expanded_url:string,indices:array<smallint>,url:string>>,user_mentions:array<struct<id:bigint,id_str:string,indices:array<smallint>,name:string,screen_name:string>>,symbols:array<struct<indices:array<smallint>,text:string>>,media:array<struct<display_url:string,expanded_url:string,id:bigint,id_str:string,indices:array<smallint>,media_url:string,media_url_https:string,source_status_id:bigint,source_status_id_str:string,type:string,url:string>>>>,retweeted_status:struct<created_at:timestamp,id:bigint,id_str:string,text:string,source:string,truncated:boolean,in_reply_to_status_id:bigint,in_reply_to_status_id_str:string,in_reply_to_user_id:bigint,in_reply_to_user_id_str:string,in_reply_to_screen_name:string,quoted_status_id:bigint,quoted_status_id_str:string,is_quote_status:boolean,retweet_count:int,favorite_count:int,favorited:boolean,retweeted:boolean,possibly_sensitive:boolean,filter_level:string,lang:string,user:struct<id:bigint,id_str:string,name:string,screen_name:string,location:string,url:string,description:string,protected:boolean,verified:boolean,followers_count:int,friends_count:int,listed_count:int,favourites_count:int,statuses_count:int,created_at:timestamp,profile_banner_url:string,profile_image_url_https:string,default_profile:boolean,default_profile_image:boolean,withheld_in_countries:array<string>,withheld_scope:string>,coordinates:struct<coordinates:array<float>,type:string>,place:struct<id:string,url:string,place_type:string,name:string,full_name:string,country_code:string,country:string,bounding_box:struct<coordinates:array<array<array<float>>>,type:string>>,entities:struct<hashtags:array<struct<indices:array<smallint>,text:string>>,urls:array<struct<display_url:string,expanded_url:string,indices:array<smallint>,url:string>>,user_mentions:array<struct<id:bigint,id_str:string,indices:array<smallint>,name:string,screen_name:string>>,symbols:array<struct<indices:array<smallint>,text:string>>,media:array<struct<display_url:string,expanded_url:string,id:bigint,id_str:string,indices:array<smallint>,media_url:string,media_url_https:string,source_status_id:bigint,source_status_id_str:string,type:string,url:string>>>>>\""


# TODO: Comment this file.
# TODO: write README
def main():
    # Configure logging module to save on log file and present messages on the screen too
    logging.basicConfig(filename=os.path.join(os.path.dirname(__file__), 'upload.log'),
                        filemode='w',
                        format='%(asctime)s - %(levelname)s - %(message)s',
                        level=logging.INFO)
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console.setFormatter(formatter)
    logging.getLogger('').addHandler(console)
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

    logging.info('Going to create index...')
    connection.execute('create index if not exists idx_tweet on tweet(project, creation_date, tweet_id)')

    logging.info('Going to read config.ini...')
    config = configparser.ConfigParser()
    config.read(os.path.join(os.path.dirname(__file__), 'config.ini'))

    logging.info('Going to connect to S3...')
    session = boto3.Session(
        aws_access_key_id=config['aws']['aws_access_key_id'],
        aws_secret_access_key=config['aws']['aws_secret_access_key'],
        region_name=config['aws']['region']
    )
    s3 = session.resource('s3')

    logging.info('Going to create S3 directory...')
    directory = os.path.join(os.path.dirname(__file__), 's3')
    os.makedirs(directory, exist_ok=True)
    filename_json = os.path.join(directory, 'temp.json')
    filename_orc = os.path.join(directory, 'temp.orc')
    filename_bz2 = os.path.join(directory, 'temp.json.bz2')

    cursor_files = connection.cursor()
    cursor_records = connection.cursor()

    logging.info('Going to execute outer query...')
    cursor_files.execute('select project, creation_date, count(*) as num_records '
                         'from tweet '
                         'group by project, creation_date '
                         'order by project, creation_date')

    for file in cursor_files:
        logging.info('Processing data for project %s and creation date %s...', file['project'], file['creation_date'])
        logging.info('Going to create file %s...', filename_json)
        with open(filename_json, 'w') as json_file:
            logging.info('Going to execute inner query...')
            cursor_records.execute('select tweet_id, tweet_json '
                                   'from tweet '
                                   'where project = ? and creation_date = ? '
                                   'order by tweet_id', (file['project'], file['creation_date']))
            logging.info('Going to import data from sqlite to json file...')
            for record in cursor_records:
                json_file.write("{}\n".format(record['tweet_json']))

        logging.info('Going to compress JSON file...')
        with open(filename_json, 'rb') as input_file:
            with bz2.BZ2File(filename_bz2, 'wb', compresslevel=9) as output_file:
                copyfileobj(input_file, output_file)

        filename_s3 = 'twitter_stream/project={}/creation_date={}/{}.json.bz2'.format(
            file['project'], file['creation_date'], str(file['num_records']))
        logging.info('Going to upload bz2 file to bucket %s as %s...', config['aws']['s3_bucket_raw'], filename_s3)
        s3.Bucket(config['aws']['s3_bucket_raw']).upload_file(filename_bz2, filename_s3)
        logging.info('Going to delete temporary bz2 file...')
        os.remove(filename_bz2)

        logging.info('Going to convert into ORC format...')
        try:
            os.remove(filename_orc)
        except OSError:
            pass
        subprocess.run(['java',
                        '-jar', os.path.join(os.path.dirname(__file__), 'orc-tools-1.5.6-uber.jar'),
                        'convert', filename_json,
                        '-o', filename_orc,
                        '-s', STRUCTURE_TWEET],
                       check=True)
        logging.info('Going to delete temporary JSON file...')
        os.remove(filename_json)
        filename_s3 = 'twitter_stream/project={}/creation_date={}/{}.orc'.format(
            file['project'], file['creation_date'], str(file['num_records']))
        logging.info('Going to upload ORC file to bucket %s as %s...', config['aws']['s3_bucket'], filename_s3)
        s3.Bucket(config['aws']['s3_bucket']).upload_file(filename_orc, filename_s3)
        logging.info('Going to delete temporary ORC file...')
        os.remove(filename_orc)

    logging.info('Going to drop table tweet...')
    connection.execute('drop table tweet')
    logging.info('Going to recreate table tweet...')
    connection.execute("create table if not exists tweet"
                       "(project string,"
                       "creation_date timestamp,"
                       "tweet_id string,"
                       "tweet_json string)")
    logging.info('Going to vacuum sqlite file...')
    connection.execute("vacuum")
    connection.close()

    athena = session.client('athena', region_name=config['aws']['region'])
    logging.info('Going to delete athena tables for twitter_stream...')
    athena.start_query_execution(
        QueryString="drop table if exists twitter_stream",
        QueryExecutionContext={'Database': 'internetscholar'},
        ResultConfiguration={'OutputLocation': 's3://internetscholar-temp/twitter_stream'}
    )
    athena.start_query_execution(
        QueryString="drop table if exists twitter_stream_raw",
        QueryExecutionContext={'Database': 'internetscholar'},
        ResultConfiguration={'OutputLocation': 's3://internetscholar-temp/twitter_stream'}
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
        """,
        QueryExecutionContext={'Database': 'internetscholar'},
        ResultConfiguration={'OutputLocation': 's3://internetscholar-temp/twitter_stream'}
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
        """,
        QueryExecutionContext={'Database': 'internetscholar'},
        ResultConfiguration={'OutputLocation': 's3://internetscholar-temp/twitter_stream'}
    )

    logging.info('Going to repair partitions for twitter_stream...')
    athena.start_query_execution(
        QueryString="MSCK REPAIR TABLE twitter_stream",
        QueryExecutionContext={'Database': 'internetscholar'},
        ResultConfiguration={'OutputLocation': 's3://internetscholar-temp/twitter_stream'}
    )
    athena.start_query_execution(
        QueryString="MSCK REPAIR TABLE twitter_stream_raw",
        QueryExecutionContext={'Database': 'internetscholar'},
        ResultConfiguration={'OutputLocation': 's3://internetscholar-temp/twitter_stream'}
    )

    logging.info('End.')


if __name__ == '__main__':
    main()
