import logging
from ignis.rpc.executor.keys import IKeysModule as IKeysModuleRpc
from .IModule import IModule, IRawIndexMemoryObject
from ..storage.iterator.ICoreIterator import readToWrite
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
				msgObject = self.getIObject(elems=int(obj.getSize() / len(executorKeys)) + 1)
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
			size = obj.getSize()

			reader = obj.readIterator()
			parts = list()
			div = int(size / workers)
			mod = size % workers
			if workers == 1 or type(obj) == IRawIndexMemoryObject:
				for t in range(0, workers):
					parts.append((obj, t * div + (t if t < mod else mod)))
			else:
				for t in range(0, workers):
					partObj = self.getIObject(div + 1)
					readToWrite(reader, partObj.writeIterator(), div + (1 if t < mod else 0))
					parts.append((partObj, 0))
				obj = parts[0][0]

			def keySplit(t, partObj, skip):
				objs = dict()
				writers = dict()
				reader = partObj.readIterator()
				reader.skip(skip)
				localSize = div + (1 if mod > t else 0)

				for i in range(0, localSize):
					tuple = reader.next()
					key = tuple[0]
					value = tuple[1]
					hash = IHash(key)
					writer = writers.get(hash, None)
					if writer is None:
						aux = IKeysModule.getIObjectStatic(context)
						objs[hash] = aux
						writer = aux.writeIterator()
						writers[hash] = writer
					writer.write(value)
				return objs

			def mergeSplits(d1, d2):
				result = d1
				for key in d2:
					if key in result:
						d2[key].moveTo(result[key])
					else:
						result[key] = d2[key]
				return result

			def reduce(key, values):
				reader = values.readIterator()
				result = reader.next()
				while reader.hasNext():
					result = f.call(result, reader.next(), context)
				values.clear()
				values.writeIterator().write((key, result))
				return values

			logger.info("IKeysModule creating " + str(workers) + " threads")

			f.before(context)
			if workers > 1:
				with IProcessPoolExecutor(workers - 1) as pool:
					results = list()
					for i in range(0, workers):
						results.append(pool.submit(keySplit, i, parts[i][0], parts[i][1]))
					while len(results) > 1:
						results.append(pool.submit(mergeSplits, results.pop().result(), results.pop().result()))

					objs = results.pop().result()
					for key, values in objs.items():
						results.append(pool.submit(reduce, key.obj, values))

					objOut = results[0].result()
					for i in range(1, len(results)):
						results[i].result().moveTo(objOut)
			else:
				objs = keySplit(0, obj, 0)
				keys = len(objs)
				objOut = self.getIObject(elems=keys)
				for key, values in objs.items():
					reduce(key.obj, values).moveTo(objOut)
			f.after(context)

			self._executorData.loadObject(objOut)
			logger.info("IKeysModule reduceByKey ready")
		except Exception as ex:
			self.raiseRemote(ex)
