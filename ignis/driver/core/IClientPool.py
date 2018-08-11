import threading

from ignis.driver.core.IClient import IClient

class IClientPool:
	def __init__(self, host, port):
		self.__host = host
		self.__port = port
		self.__bound = set()
		self.__free = list()
		self.__lock = threading.Lock()

	def destroy(self):
		with self.__lock:
			for client in self.__bound:
				client._close()
			for client in self.__free:
				client._close()
			self.__free.clear()
			self.__bound.clear()

	def _getClient(self):
		client = None
		with self.__lock:
			if not self.__free:
				client = IClient(self.__host, self.__port)
			else:
				client = self.__free.pop()
			self.__bound.add(client)
		return client

	def _returnClient(self, client):
		with self.__lock:
			self.__bound.remove(client)
			self.__free.append(client)

	def client(self):
		return BoundClient(self)


class BoundClient:
	def __init__(self, pool):
		self.__pool = pool
		self.__client = None

	def __enter__(self):
		self.__client = self.__pool._getClient()
		return self.__client

	def __exit__(self, exc_type, exc_val, exc_tb):
		self.__pool._returnClient(self.__client)
		self.__client = None
