import logging
from ignis.rpc.executor.mapper import IMapperModule as IMapperModuleRpc
from .IModule import IModule
from ignis.executor.core.IParallelFork import IParallelFork

logger = logging.getLogger(__name__)


class IMapperModule(IModule, IMapperModuleRpc.Iface):

	def __init__(self, executorData):
		super().__init__(executorData)

	def __pipe(self, sf, action):
		try:
			f = self.loadSource(sf)
			threads = self._executorData.getThreads()
			obj = self._executorData.loadObject()
			if threads > 1:
				obj = self.memoryObject(obj)
				self._executorData.loadObject(obj)
				objOutl = [self.memoryObject() for i in range(0, threads)]
			else:
				objOutl = (self.getIObject(),)

			size = obj.getSize()
			context = self._executorData.getContext()
			reader = obj.readIterator()

			f.before(context)
			logger.info("IMapperModule creating " + str(threads) + " threads")
			with IParallelFork(workers=threads) as p:
				t = p.getId()
				objOut = objOutl[t]
				div = int(size / threads)
				mod = size % threads
				localSize = div + (1 if mod > t else 0)
				skip = div * t + (t if mod > t else mod)

				writer = objOut.writeIterator()
				reader.skip(skip)
				for i in range(0, localSize):
					elem = reader.next()
					result = f.call(elem, context)
					action(elem, result, writer)
			f.after(context)

			objOut = objOutl[0]
			if threads > 1:
				objOut = self.getIObject()
				for obj in objOutl:
					obj.moveTo(objOut)

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

	def streamingMap(self, sf, ordered):
		self._map(sf)  # TODO

	def streamingFlatmap(self, sf, ordered):
		self.flatmap(sf)  # TODO

	def streamingFilter(self, sf, ordered):
		self.filter(sf)  # TODO

	def streamingKeyBy(self, sf, ordered):
		self.keyBy(sf)  # TODO
