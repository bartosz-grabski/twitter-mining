from threading import Thread
from sys import stdout
from time import sleep
from twython import TwythonStreamer
from json_socket import JSONSocket, NoMessageAvailable, ConnectionLost

import random
import json
import multiprocessing
import mongoengine as mongo
import string
import time
import math

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

        print('%d tweets in database, downloading %.4f/s' % (tweetsNow, self.downloadSpeed))


class Client(Thread):
    def waitForCoordinates(self):
        print('waiting for coordinates...')
        self.coordinates = None
        while not self.coordinates:
            try:
                msg = self.socket.recv()
                if msg['type'] == 'areaDefinition':
                    self.coordinates = msg['area']
                else:
                    print('received message:\n%s' % json.dumps(msg, indent = 4, separators = (',', ': ')))
            except NoMessageAvailable:
                pass
        print('got: %s' % self.coordinates)

    def __init__ (self, hostname, port, tweetCounter):
        Thread.__init__(self)
        self.overflow = False;
        self.socket = JSONSocket()
        self.socket.connect((hostname, port))
        self.tweetCounter = tweetCounter
        self.coordinates = None
        self.waitForCoordinates()

    def getCoordinates(self):
        return self.coordinates

    def run(self):
        while (True):
            # po zapytaniu servera czy zyjemy pasuje mu odpowiedziec ( i powiedziec czy mamy przepelnienei czy nie),
            # serwer moze nam powiedziec bysmy zmienili wspolrzedne po ktorych "szukamy"
            self.tweetCounter.count()
            self.socket.send({
                'type': 'statusReport',
                'downloadSpeed': self.tweetCounter.downloadSpeed
            })

            try:
                msg = self.socket.recv()
            except NoMessageAvailable:
                pass

            time.sleep(0.5)

class Streamer(TwythonStreamer):
    def __init__(self, tweetTimeQueue, *args, **kwargs):
        TwythonStreamer.__init__(self, *args, **kwargs)
        self.tweetTimeQueue = tweetTimeQueue

    def on_success(self, data):
        if 'geo' not in data:
            #print('not a tweet, skipping')
            print(json.dumps(data, indent = 4, separators = (',', ': ')))
            return

        if data['geo'] is None:
            #print('error: no geolocation')
            print(json.dumps(data, indent = 4, separators = (',', ': ')))
            return

        geo = data['geo']['coordinates']
        (Tweet(tweetid = data['id'],
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

def spawn(tweetTimeQueue, vsp):
    global twitterKeys
    twitterKey =  random.choice(twitterKeys)

    try:
        stream = Streamer(tweetTimeQueue, *twitterKey)
        stream.statuses.filter(locations = vsp)
    finally:
        pass

twitterKeys = [ x.strip().split(',') for x in open('auth').readlines() ]

mongo.connect('twitter2')
tweetTimeQueue = multiprocessing.Queue()

counter = TweetCounter(tweetTimeQueue)
client = Client('127.0.0.1', 12346, counter)
client.start()

coordinates = client.getCoordinates()

for n in range(1):
    process = multiprocessing.Process(target=spawn, args=([tweetTimeQueue], coordinates))
    process.start()

