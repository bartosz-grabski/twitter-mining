#!/usr/bin/env python

import random
import json
from twython import TwythonStreamer
import mongoengine as mongo


class InvalidMessage(Exception): pass

class GeoPoint(mongo.EmbeddedDocument):
    longitude = mongo.fields.FloatField(required = True)
    latitude = mongo.fields.FloatField(required = True)

class Tweet(mongo.Document):
    tweetid = mongo.fields.IntField(required = True)
    userid = mongo.fields.IntField(required = True)
    text = mongo.fields.StringField(required = True, max_length = 140)
    geo = GeoPoint(required = True)

class User(mongo.Document):
    userid = mongo.fields.IntField(required = True)
    name = mongo.fields.StringField(required = True)
    tags = mongo.fields.ListField(mongo.fields.StringField())

class Streamer(TwythonStreamer):
    def on_success(self, data):
        if 'geo' not in data:
            print('not a tweet, skipping')
            print(json.dumps(data, indent = 4, separators = (',', ': ')))
            return

        if data['geo'] is None:
            return

        geo = data['geo']['coordinates']
        Tweet(tweetid = data['id'],
              userid = data['user']['id'],
              text = data['text'],
              geo = GeoPoint(longitude = geo[1],
                             latitude = geo[0])
              ).save()

        print('%d tweets in database' % len(Tweet.objects))

    def on_error(self, status_code, data):
        print('ERROR: %d' % status_code)
        print(data)

        self.disconnect()

keys = [ x.strip().split(',') for x in open('auth').readlines() ]

currKey = random.choice(keys)
print('using key:')
print('\n'.join(currKey))

mongo.connect('twitter')

try:
    stream = Streamer(*currKey)
    stream.statuses.filter(track = 'bieber')
finally:
    pass

