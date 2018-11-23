from ignis.rpc.executor.server import IServerModule as IServerModuleRpc
from .IModule import IModule
from thrift.server.TServer import TSimpleServer
from thrift.transport.TSocket import TServerSocket
from thrift.protocol.TCompactProtocol import TCompactProtocolFactory
from thrift.transport.TTransport import TBufferedTransportFactory


class IServerModule(IModule, IServerModuleRpc.Iface):

	def __init__(self, executorData):
		super().__init__(executorData)
		self.__server = None

	def start(self, processor, port):
		self.__server = TSimpleServer(processor, TServerSocket(port=port), TBufferedTransportFactory(),
		                       TCompactProtocolFactory())
		self.__server.serve()

	def stop(self):
		try:
			if self.__server:
				self.__server
		except Exception as ex:
			self.raiseRemote(ex)

	def test(self):
		return True

	def updateProperties(self, properties):
		try:
			self._executorData.getContext().getProperties().update(properties)
		except Exception as ex:
			self.raiseRemote(ex)
