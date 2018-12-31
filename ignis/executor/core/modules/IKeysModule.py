import logging
from ignis.rpc.executor.keys import IKeysModule as IKeysModuleRpc
from .IModule import IModule
from ..IMessage import IMessage
from ..IProcessPoolExecutor import IProcessPoolExecutor
from ignis.data.handle.IHash import IHash

logger = logging.getLogger(__name__)


class IKeysModule(IModule, IKeysModuleRpc.Iface):

	def __init__(self, executorData):
		super().__init__(executorData)

	def getKeys(self):
		try:
			logger.info("IKeysModule starting getKeys")
			obj = self._executorData.loadObject()
			reader = obj.readIterator()
			self.__hashes = list()
			while reader.hasNext():
				self.__hashes.append(IHash.hash(reader.next()[0]))
			return self.__hashes
			logger.info("IKeysModule keys ready")
		except Exception as ex:
			self.raiseRemote(ex)

	def getKeysWithCount(self):
		try:
			pass
		except Exception as ex:
			self.raiseRemote(ex)

	def collectKeys(self):
		try:
			logger.info("IKeysModule collecting keys")
			msgs = self._executorData.getPostBox().popInBox()

			objectOut = self.getIObject()

			for id, msg in msgs.items():
				msg.getObj().moveTo(objectOut)

			self._executorData.loadObject(objectOut)
			logger.info("IKeysModule keys collected")
		except Exception as ex:
			self.raiseRemote(ex)

	def prepareKeys(self, executorKeys):
		try:
			logger.info("IKeysModule preparing keys")
			obj = self._executorData.loadObject()
			hashWriter = dict()
			size = obj.getSize()

			for entry in executorKeys:
				msgObject = self.getIObject(elems=int(obj.getSize() / len(executorKeys))+1)
				self._executorData.getPostBox().newOutMessage(entry.msg_id, IMessage(entry.addr, msgObject))
				writer = msgObject.writeIterator()
				for id in entry.keys:
					hashWriter[id] = writer

			reader = obj.readIterator()
			for i in range(0, size):
				hashWriter[self.__hashes[i]].write(reader.next())
			self.__hashes = None
			logger.info("IKeysModule keys prepared")
		except Exception as ex:
			self.raiseRemote(ex)

	def reduceByKey(self, funct):
		try:
			logger.info("IKeysModule reduceByKey starting")
			f = self.loadSource(funct)
			obj = self._executorData.loadObject()
			workers = self._executorData.getWorkers()
			context = self._executorData.getContext()
			keysAndValues = list()
			writers = dict()

			reader = obj.readIterator()
			while reader.hasNext():
				tuple = reader.next()
				key = tuple[0]
				value = tuple[1]
				writer = writers.get(IHash(key), None)
				if writer is None:
					aux = self.getIObject()
					keysAndValues.append((key, aux))
					writer = aux.writeIterator()
					writers[IHash(key)] = writer
				writer.write(value)
			del writers
			del obj
			self._executorData.deleteLoadObject()

			def work(init, end):
				localObj = IKeysModule.getIObjectStatic(context, elems=end - init)
				locaWriter = localObj.writeIterator()
				for k in range(init, end):
					selObj = keysAndValues[k]
					key = selObj[0]
					values = selObj[1].readIterator()
					result = values.next()

					while values.hasNext():
						result = f.call(result, values.next(), context)
					locaWriter.write((key, result))
				return localObj

			logger.info("IKeysModule creating " + str(workers) + " threads")
			results = list()

			f.before(context)
			size = len(keysAndValues)
			if workers > 1:
				chunkSize = int(size * (0.1 / workers)) + 1
				with IProcessPoolExecutor(workers - 1) as pool:
					for i in range(0, size + chunkSize - 1, chunkSize):
						results.append(pool.submit(work, i, min(i + chunkSize, size)))
					objOut = results[0].result()
					for i in range(1, len(results)):
						results[i].result().moveTo(objOut)
			else:
				objOut = work(0, size)
			f.after(context)

			self._executorData.loadObject(objOut)
			logger.info("IKeysModule reduceByKey ready")
		except Exception as ex:
			self.raiseRemote(ex)
