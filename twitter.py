#!/usr/bin/env python

import random
import json
from twython import TwythonStreamer

class Streamer(TwythonStreamer):
    def on_success(self, data):
        print(json.dumps(data, indent = 4, separators = (',', ': ')))

    def on_error(self, status_code, data):
        print('ERROR: %d' % status_code)
        print(data)

        self.disconnect()


keys = [ x.strip().split(',') for x in open('auth').readlines() ]

currKey = random.choice(keys)
print('using key:')
print('\n'.join(currKey))

stream = Streamer(*currKey)
stream.statuses.filter(track = 'twitter')

