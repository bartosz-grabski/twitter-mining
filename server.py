#!/usr/bin/python

from threading import Thread
from json_socket import JSONSocket, NoMessageAvailable, ConnectionLost
from data_model import AbstractTweet, GenericTweet, dbConnect
from socket import error as SocketError
import time
import json
import operator
import sys
import signal

class Puppet(object):
    def __init__(self, socket, remoteAddress, coordinates):
        self.socket = socket
        self.remoteAddress = remoteAddress
        self.coordinates = coordinates
        self.downloadSpeed = 0.0

    def shutdown(self):
        print('connection shutting down: %s:%d' % self.remoteAddress)
        self.socket.shutdown()
        self.socket.close()

class Master(Thread):
    def __init__ (self, puppets):
        Thread.__init__(self)
        self.puppets = puppets
        self.overflow = False
        self.running = True

    def run(self):
        prevTweetCount = len(GenericTweet.objects)
        prevTime = time.time()

        while self.running:
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

            tweetCount = len(GenericTweet.objects)
            now = time.time()

            print('master of %d puppets: %d tweets in databse, total download speed = %.3f/s'
                  % (len(self.puppets), len(GenericTweet.objects),
                     float(tweetCount - prevTweetCount) / (now - prevTime)))

            prevTweetCount = tweetCount
            prevTime = now

            time.sleep(1)

    def shutdown(self):
        print('master thread shutting down')
        self.running = False

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
            'type': 'databaseAddress',
            'address': self.dbAddress
        })
        clientSocket.send({
            'type': 'areaDefinition',
            'area': coords
        })

    def start(self, dbAddress, serverHost, serverPort):
        print('server listening on: %s:%d' % (serverHost, serverPort))
        print('database address: %s' % dbAddress)

        try:
            dbConnect(dbAddress)
            self.dbAddress = dbAddress
        except:
            print('fatal error: cannot connect to database')
            sys.exit(1)

        self.masterThread = Master(self.puppets)
        self.masterThread.start()

        try:
            self.serverSocket = JSONSocket()
            self.serverSocket.bind((serverHost, serverPort))
            self.serverSocket.listen(5)
        except SocketError:
            self.shutdown()
            return

        try:
            while True:
                self.init_puppet(*self.serverSocket.accept())
        except SocketError as e:
            print('\nserver shutdown. reason: %s (errno = %d)' % (e[1], e[0]))

    def shutdown(self):
        self.masterThread.shutdown()

        self.serverSocket.shutdown()
        self.serverSocket.close()

        for client in self.puppets:
            client.shutdown()

def shutdownSignalHandler(sigNum, stackFrame):
    global server
    print('caught signal: %d' % sigNum)
    server.shutdown()

signal.signal(signal.SIGINT, shutdownSignalHandler)
signal.signal(signal.SIGTERM, shutdownSignalHandler)

SERVER_HOST = '127.0.0.1'
SERVER_PORT = 12346

DB_HOST = '127.0.0.1'
DB_PORT = 27017
DB_NAME = 'twitter'

if len(sys.argv) not in [ 1, 3, 6 ]:
    print('usage: server.py server_host server_port [ db_host db_port db_name ]')
    sys.exit(1)

if len(sys.argv) >= 3:
    SERVER_HOST = sys.argv[1]
    SERVER_PORT = int(sys.argv[2])
if len(sys.argv) == 6:
    DB_HOST = sys.argv[3]
    DB_PORT = int(sys.argv[4])
    DB_NAME = sys.argv[5]

dbAddress = 'mongodb://%s:%d/%s' % (DB_HOST, DB_PORT, DB_NAME)
puppets  = []
server = Server(puppets)
server.start(dbAddress, SERVER_HOST, SERVER_PORT)

