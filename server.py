#!/usr/bin/python

from __future__ import print_function
from threading import Thread
from json_socket import JSONSocket, NoMessageAvailable, ConnectionLost
from data_model import AbstractTweet, GenericTweet, dbConnect, genericSize
from socket import error as SocketError
import time
import json
import operator
import sys
import signal
import errno

WHOLE_WORLD_COORDS = [ -180.0, -90.0, 180.0, 90.0 ]

def verbosePrint(*args, **kwargs):
    global VERBOSE
    if VERBOSE:
        print(*args, **kwargs)

def split_coordinates(coordinates):
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

class Puppet(object):
    def __init__(self, socket, remoteAddress, coordinates):
        self.socket = socket
        self.remoteAddress = remoteAddress
        self.coordinates = coordinates
        self.downloadSpeed = 0.0
        self.timeLastSeen = time.time()

    def shutdown(self):
        print('connection shutting down: %s:%d' % self.remoteAddress)
        self.socket.shutdown()
        self.socket.close()

    def isConnectionTimedOut(self):
        TIMEOUT_S = 10.0
        return time.time() - self.timeLastSeen > TIMEOUT_S

class Master(Thread):
    def __init__ (self, server):
        Thread.__init__(self)
        self.newConnections = []
        self.puppets = []
        self.disconnectedPuppets = []
        self.overflow = False
        self.running = True
        self.server = server

    def assignRegionTo(self, region, puppet):
        try:
            puppet.socket.send({
                'type': 'areaDefinition',
                'area': region
            })
            puppet.coordinates = region

            if puppet not in self.puppets:
                self.puppets.append(puppet)
        except ConnectionLost as e:
            self.markPuppetAsDisconnected(puppet)
            raise e

    def splitRegionEvenly(self, region, puppets):
        if len(puppets) == 1:
            self.assignRegionTo(region, puppets[0])
        elif len(puppets) > 1:
            subregions = split_coordinates(region)
            half = len(puppets) / 2
            self.splitRegionEvenly(subregions[0], puppets[:half])
            self.splitRegionEvenly(subregions[1], puppets[half:])

    def markPuppetAsDisconnected(self, puppet):
        print('connection lost with %s:%d' % puppet.remoteAddress)
        self.disconnectedPuppets.append(puppet)

    def handleDisconnectedPuppets(self):
        if not self.disconnectedPuppets:
            return

        puppetsCopy = []
        done = False
        while not done:
            for puppet in self.disconnectedPuppets:
                if puppet in self.puppets:
                    self.puppets.remove(puppet)
                if puppet in puppetsCopy:
                    puppetsCopy.remove(puppet)

            self.disconnectedPuppets = []

            try:
                puppetsCopy += self.puppets
                self.puppets = []
                self.splitRegionEvenly(WHOLE_WORLD_COORDS, puppetsCopy)
                done = True

                verbosePrint('regions reassigned:')
                for puppet in self.puppets:
                    verbosePrint('%s:%d: %s' % (puppet.remoteAddress +
                                                (puppet.coordinates,)))
            except ConnectionLost:
                pass

    def get_busiest_puppet(self):
        return sorted(self.puppets, key = operator.attrgetter('downloadSpeed'))[-1]

    # TODO: handle disconnected puppets here
    def assignNewPuppets(self):
        for clientSocket, clientAddr in self.newConnections:
            if not self.puppets:
                coords = WHOLE_WORLD_COORDS
            else:
                busiest = self.get_busiest_puppet()
                busiest.coordinates, coords = split_coordinates(busiest.coordinates)

                try:
                    self.assignRegionTo(busiest.coordinates, busiest)
                except ConnectionLost:
                    self.markPuppetAsDisconnected(busiest)

            puppet = Puppet(socket = clientSocket,
                            remoteAddress = clientAddr,
                            coordinates = coords)

            try:
                puppet.socket.send({
                    'type': 'databaseAddress',
                    'address': self.server.dbAddress
                })

                self.assignRegionTo(coords, puppet)
            except ConnectionLost:
                pass

        self.newConnections = []

    def run(self):
        prevTweetsSize = genericSize()
        prevTime = time.time()
        while self.running:
            self.handleDisconnectedPuppets()
            self.assignNewPuppets()

            for puppet in self.puppets:
                try:
                    try:
                        msg = puppet.socket.recv()
                        puppet.downloadSpeed = msg['downloadSpeed']
                        puppet.timeLastSeen = time.time()
                    except NoMessageAvailable:
                        if puppet.isConnectionTimedOut():
                            raise ConnectionLost('timeout')
                except ConnectionLost:
                    self.markPuppetAsDisconnected(puppet)

            self.downloadSpeed = (genericSize() - prevTweetsSize) / (time.time()-prevTime)

            verbosePrint('master of %d puppets: %d tweets in database, total download speed = %.3f/s'
                         % (len(self.puppets), genericSize(), self.downloadSpeed))
            prevTweetsSize = genericSize()
            prevTime = time.time()
            time.sleep(max(1-(time.time() - prevTime),1))

    def shutdown(self):
        print('master thread shutting down')
        self.running = False

        for client in self.puppets:
            client.shutdown()


class Server(object):
    def start(self, dbAddress, serverHost, serverPort):
        print('server listening on: %s:%d' % (serverHost, serverPort))
        print('database address: %s' % dbAddress)

        try:
            dbConnect(dbAddress)
            self.dbAddress = dbAddress
        except:
            print('fatal error: cannot connect to database')
            sys.exit(1)

        self.masterThread = Master(self)
        self.masterThread.start()

        try:
            self.serverSocket = JSONSocket()
            self.serverSocket.bind((serverHost, serverPort))
            self.serverSocket.listen(1)
        except SocketError:
            self.shutdown()
            return

        try:
            while True:
                try:
                    clientSocket, clientAddr = self.serverSocket.accept()
                    self.masterThread.newConnections.append((clientSocket, clientAddr))
                    print('connection from %s:%d' % clientAddr)
                except SocketError as e:
                    if e[0] != errno.EINTR: # interrupted system call - e.g. signal
                        raise
        except SocketError as e:
            print('\nserver shutdown. reason: %s' % e)

    def shutdown(self):
        self.masterThread.shutdown()

        self.serverSocket.shutdown()
        self.serverSocket.close()

def shutdownSignalHandler(sigNum, stackFrame):
    global server
    print('caught signal: %d' % sigNum)
    server.shutdown()

def sigusr1Handler(sigNum, stackFrame):
    global server
    print >> sys.stderr, ('master of %d puppets: %d tweets in database, total download speed = %.3f/s'
         % (len(server.masterThread.puppets),
            len(GenericTweet.objects),
            server.masterThread.downloadSpeed))

signal.signal(signal.SIGINT, shutdownSignalHandler)
signal.signal(signal.SIGTERM, shutdownSignalHandler)

signal.signal(signal.SIGUSR1, sigusr1Handler)

SERVER_HOST = '0.0.0.0'
SERVER_PORT = 12345

DB_HOST = '127.0.0.1'
DB_PORT = 27017
DB_NAME = 'twitter'

VERBOSE = False

if len(sys.argv) > 1 and sys.argv[1] == '-v':
    sys.argv.remove('-v')
    VERBOSE = True
    print('verbose mode on')

if len(sys.argv) not in [ 1, 2, 5 ]:
    print('usage: server.py [ -v ] server_port [ db_host db_port db_name ]')
    sys.exit(1)

if len(sys.argv) >= 2:
    SERVER_PORT = int(sys.argv[1])
if len(sys.argv) == 5:
    DB_HOST = sys.argv[2]
    DB_PORT = int(sys.argv[3])
    DB_NAME = sys.argv[4]

dbAddress = 'mongodb://%s:%d/%s' % (DB_HOST, DB_PORT, DB_NAME)
server = Server()
server.start(dbAddress, SERVER_HOST, SERVER_PORT)

