from ignis.rpc.executor.storage import IStorageModule as IStorageModuleRpc
from .IModule import IModule

class IStorageModule(IModule, IStorageModuleRpc.Iface):

	def __init__(self, executorData):
		super().__init__(executorData)

	def count(self):
		return self._executorData.loadObject().getSize()

	def cache(self, id, storage):
		pass

	def uncache(self, id):
		pass

	def restore(self, id):
		pass

	def saveContext(self, id):
		pass

	def loadContext(self, id):
		pass


