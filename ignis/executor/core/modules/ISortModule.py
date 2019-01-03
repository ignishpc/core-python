import logging
from ignis.rpc.executor.sort import ISortModule as ISortModuleRpc
from .IModule import IModule, IRawIndexMemoryObject
from ..storage.iterator.ICoreIterator import readToWrite
from ..IProcessPoolExecutor import IProcessPoolExecutor
from ...api.function.IFunction2 import IFunction2

logger = logging.getLogger(__name__)


class ISortModule(IModule, ISortModuleRpc.Iface):

	def __init__(self, executorData):
		super().__init__(executorData)

	def localCustomSort(self, sf, ascending):
		try:
			less = self.loadSource(sf)
			self.__mergeSort(less, ascending)
		except Exception as ex:
			self.raiseRemote(ex)

	def localSort(self, ascending):
		try:
			class NarutalOrder(IFunction2):

				def call(self, elem1, elem2, context):
					return elem1 < elem2

			self.__mergeSort(NarutalOrder(), ascending)
		except Exception as ex:
			self.raiseRemote(ex)

	def __mergeSort(self, less, ascending):
		logger.info("ISortModule sorting")
		workers = self._executorData.getWorkers()
		obj = self._executorData.loadObject()
		self._executorData.deleteLoadObject()
		size = obj.getSize()
		context = self._executorData.getContext()

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

		workers = min(workers, int(size / workers))
		workers = max(workers, 1)
		logger.info("ISortModule sorting with " + str(workers) + " threads")
		less.before(context)

		def workerSort(t, partObj, skip):
			reader = partObj.readIterator()
			reader.skip(skip)
			localSize = div + (1 if mod > t else 0)
			clears = list()
			stack = list()

			i = 0
			while True:
				while i < localSize and (len(stack) < 2 or len(stack[-1]) != len(stack[-2])):
					if len(clears) == 0:
						obj = ISortModule.getIObjectStatic(context, elems=localSize)
					else:
						obj = clears.pop()
					stack.append(obj)
					obj.writeIterator().write(reader.next())
					i += 1

				if len(stack[0]) == localSize:
					break

				r1 = stack[-1].readIterator()
				r2 = stack[-2].readIterator()
				if len(clears) == 0:
					obj = ISortModule.getIObjectStatic(context, elems=localSize)
				else:
					obj = clears.pop()
				writer = obj.writeIterator()
				aux1 = r1.next()
				aux2 = r2.next()

				while True:
					if less.call(aux1, aux2, context) == ascending:
						writer.write(aux1)
						if r1.hasNext():
							aux1 = r1.next()
						else:
							writer.write(aux2)
							readToWrite(r2, writer)
							break
					else:
						writer.write(aux2)
						if r2.hasNext():
							aux2 = r2.next()
						else:
							writer.write(aux1)
							readToWrite(r1, writer)
							break
				clears.append(stack.pop())
				clears[-1].clear()
				clears.append(stack.pop())
				clears[-1].clear()
				stack.append(obj)
			return stack[0]

		def workerMerge(merge1, merge2):
			r1 = merge1.readIterator()
			r2 = merge2.readIterator()
			obj = ISortModule.getIObjectStatic(context, elems=len(merge1) + len(merge2))
			writer = obj.writeIterator()
			aux1 = r1.next()
			aux2 = r2.next()

			while True:
				if less.call(aux1, aux2, context) == ascending:
					writer.write(aux1)
					if r1.hasNext():
						aux1 = r1.next()
					else:
						writer.write(aux2)
						readToWrite(r2, writer)
						break
				else:
					writer.write(aux2)
					if r2.hasNext():
						aux2 = r2.next()
					else:
						writer.write(aux1)
						readToWrite(r1, writer)
						break
			return obj

		merges = list()
		if workers > 1:
			with IProcessPoolExecutor(workers) as pool:
				results = list()
				for i in range(1, workers):
					results.append(pool.submit(workerSort, i, parts[i][0], parts[i][1]))
				merges.append(workerSort(0, parts[0][0], parts[0][1]))
				for r in results:
					merges.append(r.result())

				while len(merges) > 1:
					results = list()
					for i in range(3, len(merges), 2):
						results.append(pool.submit(workerMerge, merges.pop(), merges.pop()))
					merges.append(workerMerge(merges.pop(), merges.pop()))
					for r in results:
						merges.append(r.result())
		else:
			merges.append(workerSort(0, parts[0][0], parts[0][1]))
		less.after(context)
		self._executorData.loadObject(merges[0])
		logger.info("ISortModule sorted")
