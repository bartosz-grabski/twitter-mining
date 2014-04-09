#!/usr/bin/python

from threading import Thread
import socket
import pickle
import time

class Puppet:
	def __init__(self, socket, coordinates):
		self.socket = socket
		self.coordinates = coordinates

class Master(Thread):

	def __init__ (self, clientQueue):
		Thread.__init__(self)
		self.clientQueue = clientQueue
		self.overflow = False;

	def check_if_alive():
		pass

	def merge_tweetless_area():
		pass

	def break_overflow_area():
		pass
	
	def run(self):
		while (True):
			print "run them all"
			time.sleep(1)

class Server:

	def __init__(self, clientQueue):
		self.clientQueue = clientQueue
	
	def get_coordinates(self):
		# narazie tylko zeby sprawdzic czy dziala, trzeba zrobic jakos "ladnie" to
		return [-180.0, -90.0, 180.0, 90.0]

	def start_server(self, hostname, port):
		print hostname
		
		thread = Master(self.clientQueue)
		thread.start()

		s = socket.socket()
		s.bind((hostname, port))
		s.listen(5)
		while True:
			c, addr = s.accept()
			coord = self.get_coordinates()
			self.clientQueue.append( Puppet( socket = c, coordinates = coord))
			print 'connection from', addr
			c.send(pickle.dumps(coord))

clientQueue  = []

Server(clientQueue).start_server('localhost',12346)