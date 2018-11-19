import logging
from ignis.rpc.executor.postman import IPostmanModule as IPostmanModuleRpc
from .IModule import IModule

logger = logging.getLogger(__name__)


class IPostmanModule(IPostmanModuleRpc.Iface, IModule):

	def __init__(self, executorData):
		IModule.__init__(executorData)
		self.__started = False

	def start(self):
		try:
			pass

		except Exception as ex:
			self.raiseRemote(ex)

	def stop(self):
		try:
			pass

		except Exception as ex:
			self.raiseRemote(ex)

	def sendAll(self):
		try:
			pass

		except Exception as ex:
			self.raiseRemote(ex)

	def clearAll(self):
		try:
			self._executorData.getPostBox().popOutBox()
		except Exception as ex:
			self.raiseRemote(ex)
