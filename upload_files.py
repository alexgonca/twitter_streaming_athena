from datetime import datetime
import sqlite3
import os
import subprocess
import boto3
import configparser
import logging


# TODO: Comment this file.

STRUCTURE_TWEET = "\"struct<`internet_scholar`:struct<`track`:array<string>,`languages`:array<string>>,`created_at`:timestamp,`id`:bigint,`id_str`:string,`text`:string,`source`:string,`truncated`:boolean,`in_reply_to_status_id`:bigint,`in_reply_to_status_id_str`:string,`in_reply_to_user_id`:bigint,`in_reply_to_user_id_str`:string,`in_reply_to_screen_name`:string,`quoted_status_id`:bigint,`quoted_status_id_str`:string,`is_quote_status`:boolean,`retweet_count`:int,`favorite_count`:int,`favorited`:boolean,`retweeted`:boolean,`possibly_sensitive`:boolean,`filter_level`:string,`lang`:string,`user`:struct<`id`:bigint,`id_str`:string,`name`:string,`screen_name`:string,`location`:string,`url`:string,`description`:string,`protected`:boolean,`verified`:boolean,`followers_count`:int,`friends_count`:int,`listed_count`:int,`favourites_count`:int,`statuses_count`:int,`created_at`:timestamp,`profile_banner_url`:string,`profile_image_url_https`:string,`default_profile`:boolean,`default_profile_image`:boolean,`withheld_in_countries`:array<string>,`withheld_scope`:string>,`coordinates`:struct<`coordinates`:array<float>,`type`:string>,`place`:struct<`id`:string,`url`:string,`place_type`:string,`name`:string,`full_name`:string,`country_code`:string,`country`:string,`bounding_box`:struct<`coordinates`:array<array<array<float>>>,`type`:string>>,`entities`:struct<`hashtags`:array<struct<`indices`:array<smallint>,`text`:string>>,`urls`:array<struct<`display_url`:string,`expanded_url`:string,`indices`:array<smallint>,`url`:string>>,`user_mentions`:array<struct<`id`:bigint,`id_str`:string,`indices`:array<smallint>,`name`:string,`screen_name`:string>>,`symbols`:array<struct<`indices`:array<smallint>,`text`:string>>,`media`:array<struct<`display_url`:string,`expanded_url`:string,`id`:bigint,`id_str`:string,`indices`:array<smallint>,`media_url`:string,`media_url_https`:string,`source_status_id`:bigint,`source_status_id_str`:string,`type`:string,`url`:string>>>,`quoted_status`:struct<`created_at`:timestamp,`id`:bigint,`id_str`:string,`text`:string,`source`:string,`truncated`:boolean,`in_reply_to_status_id`:bigint,`in_reply_to_status_id_str`:string,`in_reply_to_user_id`:bigint,`in_reply_to_user_id_str`:string,`in_reply_to_screen_name`:string,`quoted_status_id`:bigint,`quoted_status_id_str`:string,`is_quote_status`:boolean,`retweet_count`:int,`favorite_count`:int,`favorited`:boolean,`retweeted`:boolean,`possibly_sensitive`:boolean,`filter_level`:string,`lang`:string,`user`:struct<`id`:bigint,`id_str`:string,`name`:string,`screen_name`:string,`location`:string,`url`:string,`description`:string,`protected`:boolean,`verified`:boolean,`followers_count`:int,`friends_count`:int,`listed_count`:int,`favourites_count`:int,`statuses_count`:int,`created_at`:timestamp,`profile_banner_url`:string,`profile_image_url_https`:string,`default_profile`:boolean,`default_profile_image`:boolean,`withheld_in_countries`:array<string>,`withheld_scope`:string>,`coordinates`:struct<`coordinates`:array<float>,`type`:string>,`place`:struct<`id`:string,`url`:string,`place_type`:string,`name`:string,`full_name`:string,`country_code`:string,`country`:string,`bounding_box`:struct<`coordinates`:array<array<array<float>>>,`type`:string>>,`entities`:struct<`hashtags`:array<struct<`indices`:array<smallint>,`text`:string>>,`urls`:array<struct<`display_url`:string,`expanded_url`:string,`indices`:array<smallint>,`url`:string>>,`user_mentions`:array<struct<`id`:bigint,`id_str`:string,`indices`:array<smallint>,`name`:string,`screen_name`:string>>,`symbols`:array<struct<`indices`:array<smallint>,`text`:string>>,`media`:array<struct<`display_url`:string,`expanded_url`:string,`id`:bigint,`id_str`:string,`indices`:array<smallint>,`media_url`:string,`media_url_https`:string,`source_status_id`:bigint,`source_status_id_str`:string,`type`:string,`url`:string>>>>,`retweeted_status`:struct<`created_at`:timestamp,`id`:bigint,`id_str`:string,`text`:string,`source`:string,`truncated`:boolean,`in_reply_to_status_id`:bigint,`in_reply_to_status_id_str`:string,`in_reply_to_user_id`:bigint,`in_reply_to_user_id_str`:string,`in_reply_to_screen_name`:string,`quoted_status_id`:bigint,`quoted_status_id_str`:string,`is_quote_status`:boolean,`retweet_count`:int,`favorite_count`:int,`favorited`:boolean,`retweeted`:boolean,`possibly_sensitive`:boolean,`filter_level`:string,`lang`:string,`user`:struct<`id`:bigint,`id_str`:string,`name`:string,`screen_name`:string,`location`:string,`url`:string,`description`:string,`protected`:boolean,`verified`:boolean,`followers_count`:int,`friends_count`:int,`listed_count`:int,`favourites_count`:int,`statuses_count`:int,`created_at`:timestamp,`profile_banner_url`:string,`profile_image_url_https`:string,`default_profile`:boolean,`default_profile_image`:boolean,`withheld_in_countries`:array<string>,`withheld_scope`:string>,`coordinates`:struct<`coordinates`:array<float>,`type`:string>,`place`:struct<`id`:string,`url`:string,`place_type`:string,`name`:string,`full_name`:string,`country_code`:string,`country`:string,`bounding_box`:struct<`coordinates`:array<array<array<float>>>,`type`:string>>,`entities`:struct<`hashtags`:array<struct<`indices`:array<smallint>,`text`:string>>,`urls`:array<struct<`display_url`:string,`expanded_url`:string,`indices`:array<smallint>,`url`:string>>,`user_mentions`:array<struct<`id`:bigint,`id_str`:string,`indices`:array<smallint>,`name`:string,`screen_name`:string>>,`symbols`:array<struct<`indices`:array<smallint>,`text`:string>>,`media`:array<struct<`display_url`:string,`expanded_url`:string,`id`:bigint,`id_str`:string,`indices`:array<smallint>,`media_url`:string,`media_url_https`:string,`source_status_id`:bigint,`source_status_id_str`:string,`type`:string,`url`:string>>>>>\""


def main():
    home = os.path.dirname(__file__)
    logging.basicConfig(filename=os.path.join(os.path.dirname(__file__), 'upload.log'),
                        filemode='w',
                        format='%(asctime)s - %(levelname)s - %(message)s',
                        level=logging.INFO)
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console.setFormatter(formatter)
    logging.getLogger('').addHandler(console)

    if int(datetime.utcnow().timestamp() / 86400) % 2 == 0:
        logging.info('Going to process odd.sqlite...')
        connection = sqlite3.connect('{}/db/odd.sqlite'.format(home), isolation_level=None)
    else:
        logging.info('Going to process even.sqlite...')
        connection = sqlite3.connect('{}/db/even.sqlite'.format(home), isolation_level=None)
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

    logging.info('Going to create athena directory...')
    directory = '{}/athena'.format(home)
    os.makedirs(directory, exist_ok=True)
    filename_json = '{}/temp.json'.format(directory)
    filename_orc = '{}/temp.orc'.format(directory)

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
        logging.info('Going to convert into ORC format...')
        # TODO: use CTAS instead of java tools
        subprocess.run(['java',
                        '-jar', os.path.join(os.path.dirname(__file__), 'orc-tools-1.5.6-uber.jar'),
                        'convert', filename_json,
                        '-o', filename_orc,
                        '-s', STRUCTURE_TWEET],
                       check=True)
        logging.info('Going to delete temporary JSON file...')
        # TODO: backup the JSON file to S3 before deleting it
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

    # TODO: create database if not exists, drop table if exists, create table, repair table with new partitions
    logging.info('End.')
    # TODO: github
    # TODO: write README


if __name__ == '__main__':
    main()
