from ignis.rpc.executor.server import IServerModule as IServerModuleRpc


class IServerModule():

	def __init__(self, executorData):
		pass

	def start(self, processor, port):
		pass
