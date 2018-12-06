import logging
from ignis.rpc.executor.storage import IStorageModule as IStorageModuleRpc
from .IModule import IModule

logger = logging.getLogger(__name__)


class IStorageModule(IModule, IStorageModuleRpc.Iface):

	def __init__(self, executorData):
		super().__init__(executorData)
		self.__objectsCache = dict()
		self.__objectsContext = dict()

	def count(self):
		return self._executorData.loadObject().getSize()

	def cache(self, id):
		try:
			logger.info("IStorageModule loading cache object " + str(id))
			self.__objectsCache[id] = self._executorData.loadObject()
		except Exception as ex:
			self.raiseRemote(ex)

	def uncache(self, id):
		try:
			logger.info("IStorageModule deleting cache object " + str(id))
			del self.__objectsCache[id]
		except Exception as ex:
			self.raiseRemote(ex)

	def loadCache(self, id):
		try:
			logger.info("IStorageModule loading cache object " + str(id))
			if id not in self.__objectsCache:
				raise ValueError("IStorageModule cache object not found")
			self._executorData.loadObject(self.__objectsCache[id])
		except Exception as ex:
			self.raiseRemote(ex)

	def saveContext(self, id):
		try:
			self.__objectsContext[id] = self._executorData.loadObject()
			logger.info("IStorageModule context " + str(id) + " saved")
		except Exception as ex:
			self.raiseRemote(ex)

	def loadContext(self, id):
		try:
			if id not in self.__objectsContext:
				raise ValueError("IStorageModule cache object not found")

			alreadyLoaded = self.__objectsContext[id] == self._executorData.loadObject()

			if not alreadyLoaded:
				self._executorData.loadObject(self.__objectsContext[id])
				logger.info("IStorageModule context " + str(id) + " loaded")
			del self.__objectsContext[id]
		except Exception as ex:
			self.raiseRemote(ex)

	def take(self, n, light):
		pass  # TODO

	def takeSample(self, n, withRemplacement, seed, light):
		pass  # TODO

	def collect(self, light):
		pass  # TODO
