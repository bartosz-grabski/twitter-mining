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

class Tweet(mongo.Document):
    tweetid = mongo.fields.IntField(required = True)
    userid = mongo.fields.IntField(required = True)
    text = mongo.fields.StringField(required = True, max_length = 200)
    geo = mongo.fields.ListField(mongo.fields.FloatField(), required = True)

class User(mongo.Document):
    userid = mongo.fields.IntField(required = True)
    name = mongo.fields.StringField(required = True)
    tags = mongo.fields.ListField(mongo.fields.StringField())


class TweetCounter:
    def __init__(self):
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

    def __init__(self, hostname, port, tweetCounter, limitNoticeQueue):
        Thread.__init__(self)
        self.overflow = False;
        self.socket = JSONSocket()
        self.socket.connect((hostname, port))
        self.tweetCounter = tweetCounter
        self.coordinates = None
        self.limitNoticeQueue = limitNoticeQueue
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

            while not self.limitNoticeQueue.empty():
                noticeTime = self.limitNoticeQueue.get()
                print(noticeTime - self.lastLimitNoticeTime)
                self.lastLimitNoticeTime = noticeTime

            try:
                msg = self.socket.recv()
                # TODO: obsluga wiadomosci od serwera
            except NoMessageAvailable:
                pass

            time.sleep(0.5)

class StreamerShutdown(Exception): pass

class Streamer(TwythonStreamer):
    def __init__(self, limitNoticeQueue, locations, *args, **kwargs):
        TwythonStreamer.__init__(self, *args, **kwargs)
        self.limitNoticeQueue = limitNoticeQueue
        self.locations = locations

        self.statuses.filter(locations = self.locations)

    def on_success(self, data):
        if 'limit' in data:
            self.limitNoticeQueue.put(time.time())

        if 'geo' not in data:
            #print('not a tweet, skipping')
            print(json.dumps(data, indent = 4, separators = (',', ': ')))
            return

        if data['geo'] is None:
            #print('error: no geolocation')
            #print(json.dumps(data, indent = 4, separators = (',', ': ')))
            return

        (Tweet(tweetid = data['id'],
               userid = data['user']['id'],
               text = data['text'],
               geo = data['geo']['coordinates'])
        ).save()

    def on_error(self, status_code, data):
        print('ERROR: %d' % status_code)
        print(data)

        if status_code == 420:
            print('420 error received, restarting a 90 second wait period')

        self.disconnect()
        time.sleep(90)
        self.statuses.filter(locations = self.locations)

def spawn(limitNoticeQueue, vsp):
    global twitterKeys
    twitterKey =  random.choice(twitterKeys)

    while True:
        try:
            stream = Streamer(limitNoticeQueue, vsp, *twitterKey)
        except StreamerShutdown:
            print('streamer shutting down')
            return
        except Exception as e:
            print(e)
            print('error occurred, restarting streamer')

twitterKeys = [ x.strip().split(',') for x in open('auth').readlines() ]

mongo.connect('twitter')
limitNoticeQueue = multiprocessing.Queue()

client = Client('127.0.0.1', 12346, TweetCounter(), limitNoticeQueue)
client.start()

coordinates = client.getCoordinates()

for n in range(1):
    process = multiprocessing.Process(target=spawn, args=([limitNoticeQueue], coordinates))
    process.start()

