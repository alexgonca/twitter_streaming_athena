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


# TODO: comment this file.

def gen_dict_extract(key, var):
    if hasattr(var, 'items'):
        for k, v in var.items():
            if k == key:
                yield v
            if isinstance(v, dict):
                for result in gen_dict_extract(key, v):
                    yield result
            elif isinstance(v, list):
                for d in v:
                    for result in gen_dict_extract(key, d):
                        yield result


class MyStreamListener(tweepy.StreamListener):

    def __init__(self, project, track, languages):
        super().__init__()

        self.project = project
        self.track = track
        self.languages = languages

        home = os.path.dirname(__file__)
        os.makedirs('{}/db/'.format(home), exist_ok=True)
        self.even = sqlite3.connect('{}/db/even.sqlite'.format(home), isolation_level=None)
        self.even.execute("create table if not exists tweet"
                          "(project string,"
                          "creation_date timestamp,"
                          "tweet_id string,"
                          "tweet_json string)")
        self.odd = sqlite3.connect('{}/db/odd.sqlite'.format(home), isolation_level=None)
        self.odd.execute("create table if not exists tweet"
                         "(project string,"
                         "creation_date timestamp,"
                         "tweet_id string,"
                         "tweet_json string)")

    def on_status(self, status):
        tweet_json = dict(status._json)
        tweet_json['internet_scholar'] = dict()
        tweet_json['internet_scholar']['track'] = self.track
        tweet_json['internet_scholar']['languages'] = self.languages
        creation_date = datetime.strptime(tweet_json['created_at'], '%a %b %d %H:%M:%S +0000 %Y')
        creation_date_str = datetime.strftime(creation_date, '%Y-%m-%d')
        tweet_id = tweet_json['id_str']

        json_line = json.dumps(tweet_json)
        for created_at in gen_dict_extract('created_at', tweet_json):
            json_line = json_line.replace(created_at,
                                          time.strftime('%Y-%m-%d %H:%M:%S',
                                                        time.strptime(created_at, '%a %b %d %H:%M:%S +0000 %Y')),
                                          1)

        if int(creation_date.replace(tzinfo=timezone.utc).timestamp() / 86400) % 2 == 0:
            self.even.execute('insert into tweet'
                              '(project, creation_date, tweet_id, tweet_json)'
                              'values (?, ? ,?, ?)',
                              (self.project,
                               creation_date_str,
                               tweet_id,
                               json_line))
        else:
            self.odd.execute('insert into tweet'
                             '(project, creation_date, tweet_id, tweet_json)'
                             'values (?, ? ,?, ?)',
                             (self.project,
                              creation_date_str,
                              tweet_id,
                              json_line))


def main():
    logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)

    parser = argparse.ArgumentParser(description='Collect tweets using the Streaming API.')
    parser.add_argument('-p', '--project', help='<Required> Project name', required=True)
    parser.add_argument('-t', '--track', nargs='+', help='<Required> Track terms', required=True)
    parser.add_argument('-l', '--languages', nargs='+', help='Languages', default=[])

    args = parser.parse_args()

    config = configparser.ConfigParser()
    config.read(os.path.join(os.path.dirname(__file__), 'config.ini'))
    logging.info('Read parameters.')

    # TODO: create a recursive function. 10 attempts?
    auth = tweepy.OAuthHandler(consumer_key=config['twitter']['consumer_key'],
                               consumer_secret=config['twitter']['consumer_secret'])
    auth.set_access_token(key=config['twitter']['key'],
                          secret=config['twitter']['secret'])
    api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
    logging.info('Twitter authenticated.')
    my_stream_listener = MyStreamListener(project=args.project, track=args.track, languages=args.languages)
    my_stream = tweepy.Stream(auth=api.auth, listener=my_stream_listener)
    logging.info('Listening tweets...')
    my_stream.filter(track=args.track, languages=args.languages)


if __name__ == '__main__':
    main()
