from ignis.rpc.executor.shuffle import IShuffleModule as IShuffleModuleRpc
from .IModule import IModule


class IShuffleModule(IModule, IShuffleModuleRpc.Iface):

	def __init__(self, executorData):
		super().__init__(executorData)

	def createSplits(self, splits):
		try:
			raise NotImplementedError(" python shuffle no implemented yet")  # TODO
		except Exception as ex:
			self.raiseRemote(ex)

	def joinSplits(self, order):
		try:
			raise NotImplementedError(" python shuffle no implemented yet")  # TODO
		except Exception as ex:
			self.raiseRemote(ex)
