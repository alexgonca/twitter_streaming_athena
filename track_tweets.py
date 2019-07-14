import argparse
import tweepy
import time
from datetime import timezone
from datetime import datetime
import os
import json
import sqlite3
import configparser
import logging
import boto3


# global constant and global variable that will be used to guarantee that we don't get into an infinite loop of errors
MAX_ATTEMPTS = 10
num_exceptions = 0


# class to handle the stream of tweets
class MyStreamListener(tweepy.StreamListener):

    # the constructor receives the project because it will be part of the record that will store each tweet
    # and will serve to partition the table twitter_streaming
    def __init__(self, project):
        super().__init__()

        self.project = project

        # create both even and odd databases.
        database_dir = os.path.join(os.path.dirname(__file__), 'db')
        os.makedirs(database_dir, exist_ok=True)
        self.even = sqlite3.connect(os.path.join(database_dir, 'even.sqlite'), isolation_level=None)
        self.even.execute("create table if not exists tweet"
                          "(project string,"
                          "creation_date timestamp,"
                          "tweet_id string,"
                          "tweet_json string)")
        self.odd = sqlite3.connect(os.path.join(database_dir, 'odd.sqlite'), isolation_level=None)
        self.odd.execute("create table if not exists tweet"
                         "(project string,"
                         "creation_date timestamp,"
                         "tweet_id string,"
                         "tweet_json string)")

    # method that is executed for each tweet that arrives through the stream
    def on_status(self, status):
        global num_exceptions

        # create a copy of the tweet that has been just received as a Python dict
        tweet_json = dict(status._json)

        # extract the creation date that will be part of the record that will store the tweet and it will also
        # be used to partition the table twitter_streaming
        creation_date = datetime.strptime(tweet_json['created_at'], '%a %b %d %H:%M:%S +0000 %Y')
        creation_date_str = datetime.strftime(creation_date, '%Y-%m-%d')

        # extract the tweet_id that can be used to sort the temporary table
        tweet_id = tweet_json['id_str']

        # update all fields created_at to the PrestoDB/Athena date format
        tweet_json['created_at'] = time.strftime('%Y-%m-%d %H:%M:%S',
                                                 time.strptime(tweet_json['created_at'], '%a %b %d %H:%M:%S +0000 %Y'))
        try:
            # noinspection PyTypeChecker
            tweet_json['user']['created_at'] = time.strftime('%Y-%m-%d %H:%M:%S',
                                                             time.strptime(tweet_json['user']['created_at'],
                                                                           '%a %b %d %H:%M:%S +0000 %Y'))
        except KeyError:
            pass

        try:
            # noinspection PyTypeChecker
            tweet_json['quoted_status']['created_at'] =\
                time.strftime('%Y-%m-%d %H:%M:%S',
                              time.strptime(tweet_json['quoted_status']['created_at'], '%a %b %d %H:%M:%S +0000 %Y'))
        except KeyError:
            pass

        try:
            # noinspection PyTypeChecker
            tweet_json['retweeted_status']['created_at'] =\
                time.strftime('%Y-%m-%d %H:%M:%S',
                              time.strptime(tweet_json['retweeted_status']['created_at'], '%a %b %d %H:%M:%S +0000 %Y'))
        except KeyError:
            pass

        # converts dict into string
        json_line = json.dumps(tweet_json)

        # if the number of days since Jan 1, 1970 is an even number, inserts new tweet on database "even"
        if int(creation_date.replace(tzinfo=timezone.utc).timestamp() / 86400) % 2 == 0:
            self.even.execute('insert into tweet'
                              '(project, creation_date, tweet_id, tweet_json)'
                              'values (?, ? ,?, ?)',
                              (self.project,
                               creation_date_str,
                               tweet_id,
                               json_line))
        # otherwise, inserts new tweet on database "odd"
        else:
            self.odd.execute('insert into tweet'
                             '(project, creation_date, tweet_id, tweet_json)'
                             'values (?, ? ,?, ?)',
                             (self.project,
                              creation_date_str,
                              tweet_id,
                              json_line))

        # If it is able to receive at least one tweet, reinitialize global variable num_exceptions.
        num_exceptions = 0


# recursive function that was created to guarantee that, in case of error, it will try again for MAX_ATTEMPTS times
def twitter_listening(args):
    global num_exceptions
    try:
        # Authenticate with Twitter Stream API
        config = configparser.ConfigParser()
        config.read(os.path.join(os.path.dirname(__file__), 'config.ini'))
        logging.info('Read parameters.')
        auth = tweepy.OAuthHandler(consumer_key=config['twitter']['consumer_key'],
                                   consumer_secret=config['twitter']['consumer_secret'])
        auth.set_access_token(key=config['twitter']['key'],
                              secret=config['twitter']['secret'])
        api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
        logging.info('Twitter authenticated.')

        # Initialize stream
        my_stream_listener = MyStreamListener(project=args.project)
        my_stream = tweepy.Stream(auth=api.auth, listener=my_stream_listener)
        logging.info('Listening tweets...')
        my_stream.filter(track=args.track, languages=args.languages)
    # For any exception try to reinitialize the stream MAX_ATTEMPTS times before exiting
    except Exception as e:
        if num_exceptions > MAX_ATTEMPTS:
            logging.info('It is going to terminate: %s.', repr(e))
            raise
        else:
            num_exceptions = num_exceptions + 1
            logging.info('Exception number %d: %s', num_exceptions, repr(e))
            twitter_listening(args)


# Function to save the parameters of the current project (language, track terms, creation_date) on S3
def save_project(args):
    # if temporary local s3 directory does not exist, create it
    logging.info('Going to create S3 directory...')
    directory = os.path.join(os.path.dirname(__file__), 's3')
    os.makedirs(directory, exist_ok=True)

    # create a string with all the information that we want to upload
    project_json = {
        'name': args.project,
        'track': args.track,
        'languages': args.languages,
        'created_at':  time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime())
    }
    json_line = json.dumps(project_json)

    # save the information to local file
    filename_json = os.path.join(directory, '{}.json'.format(args.project))
    with open(filename_json, 'w') as json_file:
        json_file.write("{}\n".format(json_line))

    # connect to S3 and upload the file
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
    filename_s3 = 'project/{}.json'.format(args.project)
    logging.info('Going to upload project Json file to bucket %s as %s...', config['aws']['s3_bucket_raw'], filename_s3)
    s3.Bucket(config['aws']['s3_bucket_raw']).upload_file(filename_json, filename_s3)

    # deletes temporary file
    logging.info('Going to delete temporary Json file...')
    os.remove(filename_json)


def main():
    # Configures logging package
    logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)

    # Configures argparse arguments
    parser = argparse.ArgumentParser(description='Collect tweets using the Streaming API.')
    parser.add_argument('-p', '--project', help='<Required> Project name', required=True)
    parser.add_argument('-t', '--track', nargs='+', help='<Required> Track terms', required=True)
    parser.add_argument('-l', '--languages', nargs='+', help='Languages', default=[])
    args = parser.parse_args()

    # Save project parameters on S3
    save_project(args)

    # Start collecting tweets
    twitter_listening(args)


if __name__ == '__main__':
    main()
