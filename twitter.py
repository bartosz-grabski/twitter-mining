import random
import json
from sys import stdout
from time import sleep
from twython import TwythonStreamer
import multiprocessing
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
    
    def __init__(self, tweetTimeQueue, *args, **kwargs):
        TwythonStreamer.__init__(self, *args, **kwargs)
        self.tweetTimeQueue = tweetTimeQueue

    def on_success(self, data):
        if 'geo' not in data:
            #print('not a tweet, skipping')
            #print(json.dumps(data, indent = 4, separators = (',', ': ')))
            return

        if data['geo'] is None:
            #print('error: no geolocation')
            return

        geo = data['geo']['coordinates']
        (
            Tweet(tweetid = data['id'],
              userid = data['user']['id'],
              text = data['text'],
              geo = GeoPoint(longitude = geo[1],
                             latitude = geo[0]))
        ).save()

        tweetTimeQueue.put(time.time())

    def on_error(self, status_code, data):
        print('ERROR: %d' % status_code)
        print(data)

        self.disconnect()


class TweetCounter:
    def __init__(self, tweetTimeQueue):
        self.tweetTimeQueue = tweetTimeQueue
        self.startTime = time.time()
        self.lastSecondTweets = 0
        self.downloadSpeed = 0.0
        self.tweetsBefore = len(Tweet.objects)

    def count(self):
        now = time.time()
        tweetsNow = len(Tweet.objects)
        if math.floor(now) > math.floor(self.startTime):
            deltaTime = now - self.startTime
            self.downloadSpeed = (tweetsNow - self.tweetsBefore) / deltaTime
            self.tweetsBefore = tweetsNow
            self.startTime = now

        stdout.write('\r%d tweets in database, downloading %.4f/s' % (tweetsNow, self.downloadSpeed))
        stdout.flush()

def spawn(tweetTimeQueue, vsp):
    global twitterKeys
    twitterKey = random.choice(twitterKeys)

    try:
        stream = Streamer(tweetTimeQueue, *twitterKey)
        stream.statuses.filter(locations = vsp)
    finally:
        pass

twitterKeys = [ x.strip().split(',') for x in open('auth').readlines() ]

mongo.connect('twitter2')
tweetTimeQueue = multiprocessing.Queue()

counter = TweetCounter(tweetTimeQueue)



coordinates = [ -180.0, -90.0, 180.0, 90.0]
for n in range(8):
    process = multiprocessing.Process(target=spawn, args=([tweetTimeQueue], coordinates))
    process.start()

while(True):
    counter.count()
