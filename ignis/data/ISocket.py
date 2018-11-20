from thrift.transport.TSocket import TSocket,TServerSocket, logger as socketLogger
import socket
import errno
import sys


class ISocket(TSocket):

	def __init__(self, host='localhost', port=9090, unix_socket=None, socket_family=socket.AF_UNSPEC):
		super().__init__(host, port, unix_socket, socket_family)
		socketLogger.disabled = True

	def read(self, sz):
		try:
			buff = self.handle.recv(sz)
		except socket.error as e:
			if (e.args[0] == errno.ECONNRESET and
					(sys.platform == 'darwin' or sys.platform.startswith('freebsd'))):
				self.close()
				buff = b''
			else:
				raise
		return buff

class IServerSocket(TServerSocket):

	def __init__(self, host=None, port=9090, unix_socket=None, socket_family=socket.AF_UNSPEC):
		super().__init__(host, port, unix_socket, socket_family)

	def accept(self):
		client, addr = self.handle.accept()
		result = ISocket()
		result.setHandle(client)
		return result

