import logging
import random
from ignis.rpc.executor.storage import IStorageModule as IStorageModuleRpc
from .IModule import IModule
from ..IMessage import IMessage
from ..storage.iterator.ICoreIterator import readToWrite
from ignis.data.IBytearrayTransport import IBytearrayTransport

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

	def take(self, msg_id, addr, n, light):
		try:
			logger.info("IStorageModule starting take, msg_id: " + str(msg_id)
			            + ", addr: " + str(addr)
			            + ", n: " + str(n)
			            + ", light " + str(light))
			loaded = self._executorData.loadObject()
			self._executorData.deleteLoadObject()
			object = self.getIObject()
			readToWrite(loaded.readIterator(), object.writeIterator(), n)

			_return = bytearray()
			if light:
				buffer = IBytearrayTransport(_return)
				object.write(buffer, 0)
			else:
				self._executorData.getPostBox().newOutMessage(msg_id, IMessage(addr, object))
			logger.info("IStorageModule take done")
			return _return
		except Exception as ex:
			self.raiseRemote(ex)

	def takeSample(self, msg_id, addr, n, withRemplacement, seed, light):
		try:
			logger.info("IStorageModule starting takeSample, msg_id: " + str(msg_id)
			            + ", addr: " + str(addr)
			            + ", n: " + str(n)
			            + ", withRemplacement: " + str(withRemplacement)
			            + ", seed: " + str(seed)
			            + ", light " + str(light))
			loaded = self._executorData.loadObject()
			self._executorData.deleteLoadObject()
			object = self.getIObject()
			size = len(loaded)

			reader = loaded.readIterator()
			writer = object.writeIterator()
			random.seed(seed)
			if withRemplacement:
				index = []
				for i in range(0, n):
					for j in range(0, n):
						prob = n / (size - j)
						if random.random() < prob:
							index.append(j)
							break
				index.sort()
				last = 0
				for i in index:
					reader.skip(i - last)
					writer.write(reader.next())
					last = i
			else:
				picked = 0
				for i in range(0, size):
					prob = (n - picked) / (size - i)
					if random.random() < prob:
						writer.write(reader.next())
						picked += 1
					else:
						reader.skip(1)

			_return = bytearray()
			if light:
				buffer = IBytearrayTransport(_return)
				object.write(buffer, 0)
			else:
				self._executorData.getPostBox().newOutMessage(msg_id, IMessage(addr, object))
			logger.info("IStorageModule takeSample done")
			return _return
		except Exception as ex:
			self.raiseRemote(ex)

	def collect(self, msg_id, addr, light):
		logger.info("IStorageModule starting collect, msg_id: " + str(msg_id)
		            + ", addr: " + str(addr)
		            + ", light " + str(light))
		try:
			object = self._executorData.loadObject()
			self._executorData.deleteLoadObject()
			_return = bytearray()
			if light:
				buffer = IBytearrayTransport(_return)
				object.write(buffer, 0)
			else:
				self._executorData.getPostBox().newOutMessage(msg_id, IMessage(addr, object))
			logger.info("IStorageModule collect done")
			return _return
		except Exception as ex:
			self.raiseRemote(ex)
