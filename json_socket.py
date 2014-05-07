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

    def shutdown(self):
        return self.socket.shutdown(socket.SHUT_RDWR)
    def close(self):
        return self.socket.close()

    def accept(self, *args, **kwargs):
        sock, addr = self.socket.accept(*args, **kwargs)
        return JSONSocket.wrapSocket(sock), addr

    def send(self, obj):
        if not isinstance(obj, dict):
            raise AssertionError('only dictionaries allowed for now')

        try:
            self.socket.send(json.dumps(obj))
        except IOError as e:
            err = e.args[0]
            # EAGAIN doesn't really belong here, but if the TCP buffer is full,
            # it means something really bad happened
            if err in [ errno.EPIPE, errno.ECONNRESET, errno.EAGAIN ]:
                raise ConnectionLost()
            else:
                raise e

    def recv(self):
        try:
            self.bufferedData += self.socket.recv(1024)
        except socket.error as e:
            err = e.args[0]
            if err in [ errno.EAGAIN, errno.EWOULDBLOCK ]:
                if not self.bufferedData:
                    raise NoMessageAvailable()
            elif err in [ errno.EPIPE, errno.ECONNRESET ]:
                raise ConnectionLost()
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
                break

        if endOfMessage != -1:
            msg = self.bufferedData[:endOfMessage]
            self.bufferedData = self.bufferedData[endOfMessage:]
            try:
                return json.loads(msg)
            except ValueError as e:
                print('error occurred for:')
                print(msg)
                self.bufferedData = ''

        raise NoMessageAvailable()

