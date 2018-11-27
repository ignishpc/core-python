from ignis.rpc.executor.reducer import IReducerModule as IReducerModuleRpc
from .IModule import IModule


class IReducerModule(IModule,IReducerModuleRpc.Iface):

	def __init__(self, executorData):
		super().__init__(executorData)
