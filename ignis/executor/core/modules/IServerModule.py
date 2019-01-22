import logging
from ignis.rpc.executor.server import IServerModule as IServerModuleRpc
from .IModule import IModule
from thrift.server.TServer import TServer, TTransport
from thrift.transport.TSocket import TServerSocket
from thrift.protocol.TCompactProtocol import TCompactProtocolFactory
from thrift.transport.TTransport import TBufferedTransportFactory

logger = logging.getLogger(__name__)


class IServerModule(IModule, IServerModuleRpc.Iface):
	class __ISimpleServer(TServer):
		def __init__(self, *args):
			TServer.__init__(self, *args)
			self.__closed = False

		def serve(self):
			self.serverTransport.listen()
			while not self.__closed:
				client = self.serverTransport.accept()
				if not client:
					continue
				itrans = self.inputTransportFactory.getTransport(client)
				otrans = self.outputTransportFactory.getTransport(client)
				iprot = self.inputProtocolFactory.getProtocol(itrans)
				oprot = self.outputProtocolFactory.getProtocol(otrans)
				try:
					while not self.__closed:
						self.processor.process(iprot, oprot)
				except TTransport.TTransportException:
					pass
				except Exception as x:
					logger.exception(x)

				itrans.close()
				otrans.close()

		def close(self):
			pass

	def __init__(self, executorData):
		super().__init__(executorData)
		self.__server = None

	def start(self, processor, port):
		self.__server = self.__ISimpleServer(processor, TServerSocket(port=port), TBufferedTransportFactory(),
		                                     TCompactProtocolFactory())
		self.__server.serve()

	def stop(self):
		try:
			if self.__server:
				self.__server.close()
		except Exception as ex:
			self.raiseRemote(ex)

	def test(self):
		return True

	def updateProperties(self, properties):
		try:
			self._executorData.getContext().getProperties().update(properties)
		except Exception as ex:
			self.raiseRemote(ex)
