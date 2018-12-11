import logging
from ignis.rpc.executor.keys import IKeysModule as IKeysModuleRpc
from .IModule import IModule
from ..IMessage import IMessage
from ..IParallelFork import IParallelFork
from ignis.data.handle.IHash import IHash
from multiprocessing.sharedctypes import Value
import ctypes

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
				msgObject = self.getIObject(elems=obj.getSize() / len(executorKeys))
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
			threads = self._executorData.getThreads()
			context = self._executorData.getContext()
			keysAndValues = list()
			writers = dict()

			reader = obj.readIterator()
			while reader.hasNext():
				tuple = reader.next()
				key = tuple[0]
				value = tuple[1]
				writer = writers.get(key, None)
				if writer is None:
					aux = self.memoryObject()
					keysAndValues.append((key, aux))
					writer = aux.writeIterator()
					writers[key] = writer
				writer.write(value)
			del writers
			del obj
			self._executorData.deleteLoadObject()

			if threads > 1:
				buckets = [self.memoryObject() for i in range(0, threads)]
			else:
				buckets = [self.getIObject()]
			size = len(keysAndValues)
			chunkSize = int(size * (0.1 / threads)) + 1
			ready = Value(ctypes.c_int64, chunkSize * threads, lock=False)
			f.before(context)
			with IParallelFork(workers=threads) as p:
				t = p.getId()
				resultWriter = buckets[t].writeIterator()
				init = chunkSize * t
				while init < size:
					end = min(init + chunkSize, size)
					for i in range(init, end):
						selObj = keysAndValues[i]
						key = selObj[0]
						values = selObj[1].readIterator()
						result = values.next()

						while values.hasNext():
							result = f.call(result, values.next(), context)

						resultWriter.write((key, result))

					with p.critical():
						init = ready.value
						if ready.value < size:
							ready.value += chunkSize
			f.after(context)

			objOut = buckets[0]
			if threads > 1:
				objOut = self.getIObject()
				for obj in buckets:
					obj.moveTo(objOut)

			self._executorData.loadObject(objOut)

			logger.info("IKeysModule reduceByKey ready")
		except Exception as ex:
			self.raiseRemote(ex)
