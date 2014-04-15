#!/usr/bin/python

from threading import Thread
from json_socket import JSONSocket, NoMessageAvailable, ConnectionLost
from data_model import Tweet
import time
import json
import operator

class Puppet(object):
    def __init__(self, socket, remoteAddress, coordinates):
        self.socket = socket
        self.remoteAddress = remoteAddress
        self.coordinates = coordinates
        self.downloadSpeed = 0.0

class Master(Thread):
    def __init__ (self, puppets):
        Thread.__init__(self)
        self.puppets = puppets
        self.overflow = False

    def check_if_alive():
        pass

    def merge_tweetless_area():
        pass

    def break_overflow_area():
        pass

    def run(self):
        prevTweetCount = len(Tweet.objects)
        prevTime = time.time()

        while (True):
            disconnected = []
            for puppet in self.puppets:
                try:
                    msg = puppet.socket.recv()
                    #print('recevied message:\n%s' % json.dumps(msg, indent = 4, separators = (',', ': ')))
                    puppet.downloadSpeed = msg['downloadSpeed']
                    #TODO: handle message?
                except NoMessageAvailable:
                    pass
                except ConnectionLost:
                    print('connection lost with %s:%d' % puppet.remoteAddress)
                    disconnected.append(puppet)

            for puppet in disconnected:
                self.puppets.remove(puppet)
                #TODO: make some other puppet take the disconnected one's area

            tweetCount = len(Tweet.objects)
            now = time.time()

            print('master of %d puppets: %d tweets in databse, total download speed = %.3f/s'
                  % (len(self.puppets), len(Tweet.objects),
                     float(tweetCount - prevTweetCount) / (now - prevTime)))

            prevTweetCount = tweetCount
            prevTime = now

            time.sleep(1)

class Server(object):
    def __init__(self, puppets):
        self.puppets = puppets

    def split_coordinates(self, coordinates):
        longitudeDelta = coordinates[2] - coordinates[0]
        latitudeDelta = coordinates[3] - coordinates[1]

        if longitudeDelta >= latitudeDelta:
            mid = coordinates[0] + longitudeDelta / 2.0
            return (coordinates[0:2] + [ mid, coordinates[3] ],
                    [ mid ] + coordinates[1:])
        else:
            mid = coordinates[1] + latitudeDelta / 2.0
            return (coordinates[0:3] + [ mid ],
                    [ coordinates[0], mid ] + coordinates[2:])

    def get_busiest_puppet(self):
        return sorted(self.puppets, key = operator.attrgetter('downloadSpeed'))[-1]

    def init_puppet(self, clientSocket, clientAddr):
        if not self.puppets:
            coords = [-180.0, -90.0, 180.0, 90.0]
        else:
            busiest = self.get_busiest_puppet()
            busiest.coordinates, coords = self.split_coordinates(busiest.coordinates)
            busiest.socket.send({
                'type': 'areaDefinition',
                'area': busiest.coordinates
            })

        self.puppets.append(
            Puppet(socket = clientSocket,
                   remoteAddress = clientAddr,
                   coordinates = coords)
        )

        print('connection from %s:%d' % clientAddr)
        clientSocket.send({
            'type': 'areaDefinition',
            'area': coords
        })

    def start_server(self, hostname, port):
        print hostname

        masterThread = Master(self.puppets)
        masterThread.start()

        serverSocket = JSONSocket()
        serverSocket.bind((hostname, port))
        serverSocket.listen(5)
        while True:
            self.init_puppet(*serverSocket.accept())

puppets  = []
Server(puppets).start_server('localhost', 12346)

