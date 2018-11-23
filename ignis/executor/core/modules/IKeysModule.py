from ignis.rpc.executor.keys import IKeysModule as IKeysModuleRpc
from .IModule import IModule


class IKeysModule(IModule, IKeysModuleRpc.Iface):

	def __init__(self, executorData):
		super().__init__(executorData)

	def getKeys(self):
		super().getKeys()

	def getKeysWithCount(self):
		super().getKeysWithCount()

	def prepareKeys(self, executorKeys):
		super().prepareKeys(executorKeys)

	def collectKeys(self):
		super().collectKeys()

	def reduceByKey(self, funct):
		super().reduceByKey(funct)

