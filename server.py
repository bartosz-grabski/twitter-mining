#!/usr/bin/python

from threading import Thread
from json_socket import JSONSocket, NoMessageAvailable, ConnectionLost
import time
import json

class Puppet(object):
    def __init__(self, socket, remoteAddress, coordinates):
        self.socket = socket
        self.remoteAddress = remoteAddress
        self.coordinates = coordinates

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
        while (True):
            disconnected = []
            for puppet in self.puppets:
                try:
                    msg = puppet.socket.recv()
                    print('recevied message:\n%s' % json.dumps(msg, indent = 4, separators = (',', ': ')))
                    #TODO: handle message
                except NoMessageAvailable:
                    pass
                except ConnectionLost:
                    print('connection lost with %s:%d' % puppet.remoteAddress)
                    disconnected.append(puppet)

            for puppet in disconnected:
                self.puppets.remove(puppet)
                #TODO: make some other puppet take the disconnected one's area

            time.sleep(1)

class Server(object):
    def __init__(self, puppets):
        self.puppets = puppets

    def get_coordinates(self):
        # na razie tylko zeby sprawdzic czy dziala, trzeba zrobic jakos "ladnie" to
        return [-180.0, -90.0, 180.0, 90.0]

    def start_server(self, hostname, port):
        print hostname

        thread = Master(self.puppets)
        thread.start()

        serverSocket = JSONSocket()
        serverSocket.bind((hostname, port))
        serverSocket.listen(5)
        while True:
            clientSocket, clientAddr = serverSocket.accept()
            coords = self.get_coordinates()
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

puppets  = []
Server(puppets).start_server('localhost', 12346)

