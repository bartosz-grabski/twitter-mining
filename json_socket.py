import socket
import json
import fcntl
import os
import errno

class NoMessageAvailable(Exception): pass
class ConnectionLost(Exception): pass

class JSONSocket(socket.socket):
    @staticmethod
    def wrapSocket(socket):
        jsonSocket = JSONSocket()
        jsonSocket.socket = socket
        fcntl.fcntl(jsonSocket.socket, fcntl.F_SETFL, os.O_NONBLOCK)
        return jsonSocket

    def __init__(self, *args, **kwargs):
        self.socket = socket.socket(*args, **kwargs)
        self.bufferedData = ''

    # TODO: how do I proxy?
    def connect(self, *args, **kwargs):
        ret = self.socket.connect(*args, **kwargs)
        fcntl.fcntl(self.socket, fcntl.F_SETFL, os.O_NONBLOCK)
        return ret
    def bind(self, *args, **kwargs):
        return self.socket.bind(*args, **kwargs)
    def listen(self, *args, **kwargs):
        return self.socket.listen(*args, **kwargs)

    def accept(self, *args, **kwargs):
        sock, addr = self.socket.accept(*args, **kwargs)
        return JSONSocket.wrapSocket(sock), addr

    def send(self, obj):
        if not isinstance(obj, dict):
            raise AssertionError('only dictionaries allowed for now')

        self.socket.send(json.dumps(obj))

    def recv(self):
        try:
            self.bufferedData += self.socket.recv(1024)
        except socket.error as e:
            err = e.args[0]
            if err == errno.EAGAIN or err == errno.EWOULDBLOCK:
                raise NoMessageAvailable()
            else:
                raise e

        if self.bufferedData == '':
            raise ConnectionLost()

        openingBracePos = self.bufferedData.find('{')
        if openingBracePos == -1:
            print('no opening brace in %s, skipping' % self.bufferedData)
            self.bufferedData = ''
        else:
            self.bufferedData = self.bufferedData[openingBracePos:]

        endOfMessage = -1
        braceLevel = 0
        for idx, c in enumerate(self.bufferedData):
            if c == '{':
                braceLevel += 1
            elif c == '}':
                braceLevel -= 1

            if braceLevel == 0:
                endOfMessage = idx + 1

        if endOfMessage != -1:
            msg = self.bufferedData[:endOfMessage]
            self.bufferedData = self.bufferedData[endOfMessage:]
            return json.loads(msg)

        raise NoMessageAvailable()

