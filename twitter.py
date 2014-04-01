#!/usr/bin/env python

import random
import json
from twython import TwythonStreamer
import mongoengine as mongo
import string
import time
import math

class InvalidMessage(Exception): pass

class GeoPoint(mongo.EmbeddedDocument):
    longitude = mongo.fields.FloatField(required = True)
    latitude = mongo.fields.FloatField(required = True)

class Tweet(mongo.Document):
    tweetid = mongo.fields.IntField(required = True)
    userid = mongo.fields.IntField(required = True)
    text = mongo.fields.StringField(required = True, max_length = 200)
    geo = GeoPoint(required = True)

class User(mongo.Document):
    userid = mongo.fields.IntField(required = True)
    name = mongo.fields.StringField(required = True)
    tags = mongo.fields.ListField(mongo.fields.StringField())

class Streamer(TwythonStreamer):
    def __init__(self, *args, **kwargs):
        TwythonStreamer.__init__(self, *args, **kwargs)

        self.startTime = time.time()
        self.lastSecondTweets = 0
        self.downloadSpeed = 0.0

    def on_success(self, data):
        if 'geo' not in data:
            print('not a tweet, skipping')
            print(json.dumps(data, indent = 4, separators = (',', ': ')))
            return

        if data['geo'] is None:
            print('error: no geolocation')
            return

        geo = data['geo']['coordinates']
        Tweet(tweetid = data['id'],
              userid = data['user']['id'],
              text = data['text'],
              geo = GeoPoint(longitude = geo[1],
                             latitude = geo[0])
              ).save()

        now = time.time()

        if math.floor(now) > math.floor(self.startTime):
            deltaTime = now - self.startTime
            self.downloadSpeed = float(self.lastSecondTweets) / float(deltaTime)

            self.startTime = now
            self.lastSecondTweets = 1
        else:
            self.lastSecondTweets += 1

        print('%d tweets in database, downloading %.4f/s' % (len(Tweet.objects), self.downloadSpeed))

    def on_error(self, status_code, data):
        print('ERROR: %d' % status_code)
        print(data)

        self.disconnect()

def spawn():
    global twitterKeys
    twitterKey = random.choice(twitterKeys)

    try:
        stream = Streamer(*twitterKey)
        stream.statuses.filter(locations = [ -180.0, -90.0, 180.0, 90.0 ])
    finally:
        pass

testedKeywords = set()
twitterKeys = [ x.strip().split(',') for x in open('auth').readlines() ]

mongo.connect('twitter2')

spawn()

