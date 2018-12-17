import logging
from ignis.rpc.executor.mapper import IMapperModule as IMapperModuleRpc
from ..IProcessPoolExecutor import IProcessPoolExecutor
from .IModule import IModule

logger = logging.getLogger(__name__)


class IMapperModule(IModule, IMapperModuleRpc.Iface):

	def __init__(self, executorData):
		super().__init__(executorData)

	def __pipe(self, sf, action):
		try:
			f = self.loadSource(sf)
			workers = self._executorData.getWorkers()
			obj = self._executorData.loadObject()
			obj = self.memoryObject(obj) if workers > 1 else obj
			size = obj.getSize()
			context = self._executorData.getContext()

			def work(i):
				div = int(size / workers)
				mod = size % workers
				localSize = div + (1 if mod > i else 0)
				skip = div * i + (i if mod > i else mod)
				localObj = IMapperModule.getIObjectStatic(context, elems=localSize)

				reader = obj.readIterator()
				writer = localObj.writeIterator()
				reader.skip(skip)
				for i in range(0, localSize):
					elem = reader.next()
					result = f.call(elem, context)
					action(elem, result, writer)
				return localObj

			logger.info("IMapperModule creating " + str(workers) + " threads")
			results = list()
			f.before(context)
			if workers > 1:
				with IProcessPoolExecutor(workers - 1) as pool:
					for i in range(1, workers):
						results.append(pool.submit(work, i))
					objOut = work(0)
			else:
				objOut = work(0)
			f.after(context)
			for i in range(0, workers - 1):
				results[i].result().moveTo(objOut)

			self._executorData.loadObject(objOut)
			logger.info("IMapperModule finished")
		except Exception as ex:
			self.raiseRemote(ex)

	def _map(self, sf):
		logging.info("IMapperModule starting flatmap")

		def action(elem, result, writer):
			writer.write(result)

		self.__pipe(sf, action)

	def flatmap(self, sf):
		logging.info("IMapperModule starting flatmap")

		def action(elem, result, writer):
			for elem in result:
				writer.write(elem)

		self.__pipe(sf, action)

	def filter(self, sf):
		logging.info("IMapperModule starting filter")

		def action(elem, result, writer):
			if result:
				writer.write(elem)

		self.__pipe(sf, action)

	def keyBy(self, sf):
		logging.info("IMapperModule starting keyby")

		def action(elem, result, writer):
			if result:
				writer.write((result, elem))

		self.__pipe(sf, action)

	def values(self):
		try:
			logging.info("MapperModule starting values")
			object_in = self._executorData.loadObject()
			size = len(object_in)
			object_out = self.getIObject(size)

			reader = object_in.readIterator()
			writer = object_out.writeIterator()
			for i in range(0, size):
				writer.write(reader.next()[1])

			self._executorData.loadObject(object_out)
			logging.info("IMapperModule finished")
		except Exception as ex:
			self.raiseRemote(ex)

	def streamingMap(self, sf, ordered):
		pass  # TODO

	def streamingFlatmap(self, sf, ordered):
		pass  # TODO

	def streamingFilter(self, sf, ordered):
		pass  # TODO

	def streamingKeyBy(self, sf, ordered):
		pass  # TODO
