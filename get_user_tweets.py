#!/usr/bin/env python
from twython import Twython, TwythonError

import random
import sys
from data_model import EnglishTweet, GenericTweet, dbConnect
import geohash


DEFAULT_DB_ADDRESS = "127.0.0.1"
DEFAULT_DP_PORT = 27017
DEFAULT_DB_NAME = "twitter"


def main():
    if len(sys.argv) not in [3, 6]:
        print "usage: ./get_user_tweets.py username number_of_tweets [db_host " \
              "db_port db_name]"
        sys.exit(1)
    username = sys.argv[1]
    number_of_tweets = sys.argv[2]
    db_address = _get_db_address(sys.argv)
    dbConnect(db_address)
    twython = _create_twython()
    timeline = _try_to_download_timeline(twython, username, number_of_tweets)
    _save_timeline(timeline)


def _get_db_address(args):
    if len(args) == 6:
        db_host, db_port, db_name = args[3], int(args[4]), args[5]
    else:
        db_host, db_port, db_name = \
            DEFAULT_DB_ADDRESS, DEFAULT_DP_PORT, DEFAULT_DB_NAME
    db_address = 'mongodb://%s:%d/%s' % (db_host, db_port, db_name)
    return db_address


def _create_twython():
    twitter_keys = [x.strip().split(',') for x in open('auth').readlines()]
    twython = Twython(*random.choice(twitter_keys))
    return twython


def _try_to_download_timeline(twython, username, number_of_tweets):
    try:
        timeline = twython.get_user_timeline(screen_name=username,
                                             count=number_of_tweets)
        return timeline
    except TwythonError as e:
        print "Error when downloading the timeline:"
        print e
        return []


def _save_timeline(timeline):
    for data in timeline:
        _save_tweet(data)


def _save_tweet(data):
    if 'lang' in data and data['lang'] == 'en':
        _save_english_tweet(data)
    _save_generic_tweet(data)


def _save_english_tweet(data):
    tweet = _create_tweet(EnglishTweet, data)
    tweet.save()


def _create_tweet(constructor_method, data):
    tweet = constructor_method(
        tweetid=data['id'],
        userid=data['user']['id'],
        text=data['text'],
        in_reply_to_id=data['in_reply_to_status_id'],
        username=data['user']['name'],
        screen_name=data['user']['screen_name'],
        description=data['user']['description'],
    )
    _add_geo_if_present(tweet, data)
    return tweet


def _add_geo_if_present(tweet, data):
    """
    :type tweet: data_model.AbstractTweet
    :type data: dict
    """
    if 'geo' not in data or not data["geo"]:
        print "Warning! Tweet has no geo:", tweet.tweetid, tweet.text
        return
    geo = data['geo']
    geo_hash = geohash.encode(geo['coordinates'][0],
                              geo['coordinates'][1])
    location = {'lat': geo['coordinates'][0],
                'lon': geo['coordinates'][1]}
    tweet.geohash = geo_hash
    tweet.location = location


def _save_generic_tweet(data):
    tweet = _create_tweet(GenericTweet, data)
    tweet.lang = data['lang'] if 'lang' in data else None
    tweet.save()


if __name__ == "__main__":
    main()