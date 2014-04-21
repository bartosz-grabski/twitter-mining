from sys import stdout
from time import sleep
from twython import TwythonStreamer
from json_socket import JSONSocket, NoMessageAvailable, ConnectionLost
from data_model import Tweet

import random
import json
import multiprocessing
import string
import time
import math
import traceback


class TweetCounter:
    def __init__(self):
        self.downloadSpeed = 0.0
        self.lastCheckTime = time.time()
        self.tweetAddedQueue = multiprocessing.Queue()

    def count(self):
        now = time.time()

        tweetsDownloaded = 0
        while not self.tweetAddedQueue.empty():
            self.tweetAddedQueue.get()
            tweetsDownloaded += 1

        deltaTime = now - self.lastCheckTime
        self.downloadSpeed = float(tweetsDownloaded) / deltaTime
        self.lastCheckTime = now

        print('downloading %.4f tweets/s' % self.downloadSpeed)

class StreamerShutdown(Exception): pass

class Streamer(TwythonStreamer):
    def __init__(self, tweetAddedQueue, limitNoticeQueue, locations, *args, **kwargs):
        TwythonStreamer.__init__(self, *args, **kwargs)
        self.tweetAddedQueue = tweetAddedQueue
        self.limitNoticeQueue = limitNoticeQueue
        self.locations = locations

        self.statuses.filter(locations = self.locations)

    def on_success(self, data):
        if 'limit' in data:
            self.limitNoticeQueue.put(time.time())

        if 'geo' not in data:
            #print('not a tweet, skipping')
            #print(json.dumps(data, indent = 4, separators = (',', ': ')))
            return

        if data['geo'] is None:
            #print('error: no geolocation')
            return

        (Tweet(tweetid = data['id'],
               userid = data['user']['id'],
               text = data['text'],
               in_reply_to_id=data['in_reply_to_status_id'],
               username=data['user']['name'],
               screen_name=data['user']['screen_name'],
               description=data['user']['description'],
               geo = data['geo']['coordinates'])
        ).save()

        self.tweetAddedQueue.put(1) # anything will do

    def on_error(self, status_code, data):
        print('ERROR: %d' % status_code)
        print(data)

        if status_code == 420:
            print('420 error received, restarting after a 90 second wait period')

        self.disconnect()
        time.sleep(90)
        self.statuses.filter(locations = self.locations)

class StreamerSubprocess(object):
    def __init__(self, tweetAddedQueue, coordinates):
        self.limitNoticeQueue = multiprocessing.Queue()
        self.lastLimitNoticeTime = 0.0
        self.coordinates = coordinates
        self.process = multiprocessing.Process(target=spawnStreamer,
                                               args=([ tweetAddedQueue,
                                                       self.limitNoticeQueue,
                                                       coordinates ]))
        self.process.start()

    def updateLimitNoticeTime(self):
        while not self.limitNoticeQueue.empty():
            noticeTime = self.limitNoticeQueue.get()
            print(noticeTime - self.lastLimitNoticeTime)
            self.lastLimitNoticeTime = noticeTime

    def terminate(self):
        self.process.terminate()
        #TODO: graceful exit?

class Client(object):
    def waitForCoordinates(self):
        print('waiting for coordinates...')
        self.coordinates = None
        while not self.coordinates:
            try:
                msg = self.socket.recv()
                if msg['type'] == 'areaDefinition':
                    self.resetStreamers(msg['area'])
                else:
                    print('received message:\n%s' % json.dumps(msg, indent = 4, separators = (',', ': ')))
            except NoMessageAvailable:
                pass

    def __init__(self, hostname, port):
        self.overflow = False;
        self.socket = JSONSocket()
        self.socket.connect((hostname, port))
        self.tweetCounter = TweetCounter()
        self.subprocesses = []
        self.waitForCoordinates()

    def getCoordinates(self):
        return self.coordinates

    def resetStreamers(self, coordinates):
        # TODO: podzielic jakos inteligentnie
        for subprocess in self.subprocesses:
            subprocess.terminate()
        self.subprocesses = []

        self.coordinates = coordinates
        print('gathering tweets from area: %s' % self.coordinates)
        for n in range(1):
            subprocess = StreamerSubprocess(self.tweetCounter.tweetAddedQueue,
                                            self.coordinates)
            self.subprocesses.append(subprocess)

    def run(self):
        while (True):
            # po zapytaniu servera czy zyjemy pasuje mu odpowiedziec ( i powiedziec czy mamy przepelnienei czy nie),
            # serwer moze nam powiedziec bysmy zmienili wspolrzedne po ktorych "szukamy"
            self.tweetCounter.count()
            self.socket.send({
                'type': 'statusReport',
                'downloadSpeed': self.tweetCounter.downloadSpeed
            })

            for subprocess in self.subprocesses:
                subprocess.updateLimitNoticeTime()

            # TODO: jakis komunikat o osiagnieciu limitu, jesli twitter wysle takie info

            try:
                msg = self.socket.recv()
                if msg['type'] == 'areaDefinition':
                    self.resetStreamers(msg['area'])
                # TODO: obsluga innych wiadomosci od serwera
            except NoMessageAvailable:
                pass

            time.sleep(0.5)

def spawnStreamer(tweetAddedQueue, limitNoticeQueue, vsp):
    global twitterKeys
    twitterKey = random.choice(twitterKeys)

    while True:
        try:
            stream = Streamer(tweetAddedQueue, limitNoticeQueue, vsp, *twitterKey)
        except StreamerShutdown:
            print('streamer shutting down')
            return
        except Exception as e:
            traceback.print_exc()
            print('error occurred, restarting streamer')


twitterKeys = [ x.strip().split(',') for x in open('auth').readlines() ]

client = Client('127.0.0.1', 12346)
client.run()

