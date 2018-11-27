from ignis.rpc.executor.sort import ISortModule as ISortModuleRpc
from .IModule import IModule


class ISortModule(IModule, ISortModuleRpc.Iface):

	def __init__(self, executorData):
		super().__init__(executorData)