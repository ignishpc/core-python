from ignis.rpc.executor.server import IServerModule as IServerModuleRpc
from thrift.server.TServer import TSimpleServer

class IServerModule():

	def __init__(self, executorData):
		pass

	def start(self, processor, port):
		pass
