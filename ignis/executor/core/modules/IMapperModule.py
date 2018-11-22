import logging
from ignis.rpc.executor.mapper import IMapperModule as IMapperModuleRpc
from .IModule import IModule
from ignis.executor.core.IParallelFork import IParallelFork

logger = logging.getLogger(__name__)


def _parallel_zone(sf, obj, objOut, skip, size, context):
	f = IModule.loadSource(sf)
	reader = obj.readIterator()
	writer = objOut.writeIterator()
	reader.skip(skip)
	for i in range(0, size):
		elem = reader.next()
		elem = f.call(elem, context)
		writer.write(elem)


def __parallel_zone(sf, obj, objOut, skip, size, context):
	f = IModule.loadSource(sf)



class IMapperModule(IModule, IMapperModuleRpc.Iface):

	def __init__(self, executorData):
		super().__init__(executorData)

	def _map(self, sf):
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

			f.before(context)
			logger.info("IMapperModule creating " + str(threads) + " threads")
			with IParallelFork(workers=threads) as p:
				t = p.getId()
				objOut = objOutl[t]
				div = int(size / threads)
				mod = size % threads
				localSize = div + (1 if mod > t else 0)
				skip = div * t + (t if mod > t else mod)

				reader = obj.readIterator()
				writer = objOut.writeIterator()
				reader.skip(skip)
				for i in range(0, localSize):
					elem = reader.next()
					elem = f.call(elem, context)
					writer.write(elem)
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

	def flatmap(self, sf):
		pass

	def filter(self, sf):
		pass

	def keyBy(self, sf):
		pass

	def streamingMap(self, sf, ordered):
		pass

	def streamingFlatmap(self, sf, ordered):
		pass

	def streamingFilter(self, sf, ordered):
		pass

	def streamingKeyBy(self, sf, ordered):
		pass
