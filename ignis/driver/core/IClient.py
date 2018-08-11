import time
from thrift.transport.TSocket import TSocket, logger as socket_logger
from thrift.protocol.TCompactProtocol import TCompactProtocol
from thrift.protocol.TMultiplexedProtocol import TMultiplexedProtocol
from thrift.transport.TTransport import TBufferedTransport

from ignis.rpc.driver.backend import IBackendService
from ignis.rpc.driver.cluster import IClusterService
from ignis.rpc.driver.data import IDataService
from ignis.rpc.driver.job import IJobService
from ignis.rpc.driver.properties import IPropertiesService


class IClient:

	def __init__(self, host, port):
		self.__transport = TBufferedTransport(TSocket(host, port))
		socket_logger.disabled = True  # Avoid reconnection errors
		for i in range(0, 10):
			try:
				self.__transport.open()
				break
			except Exception as ex:
				time.sleep(1)
				if i == 9:
					raise ex
		protocol = TCompactProtocol(self.__transport)
		self.__backend = IBackendService.Client(TMultiplexedProtocol(protocol, "backend"))
		self.__cluster = IClusterService.Client(TMultiplexedProtocol(protocol, "cluster"))
		self.__data = IDataService.Client(TMultiplexedProtocol(protocol, "data"))
		self.__job = IJobService.Client(TMultiplexedProtocol(protocol, "job"))
		self.__properties = IPropertiesService.Client(TMultiplexedProtocol(protocol, "properties"))

	def getIBackendService(self):
		return self.__backend

	def getIClusterService(self):
		return self.__cluster

	def getIDataService(self):
		return self.__data

	def getIJobService(self):
		return self.__job

	def getIPropertiesService(self):
		return self.__properties

	def _close(self):
		self.__transport.close()
