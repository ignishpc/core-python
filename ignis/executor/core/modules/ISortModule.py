import logging
from ignis.rpc.executor.sort import ISortModule as ISortModuleRpc
from .IModule import IModule, IRawIndexMemoryObject, IMemoryObject
from ..IMessage import IMessage
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
						obj = ISortModule.getIObjectStatic(context, elems=localSize, shared=False)
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
					obj = ISortModule.getIObjectStatic(context, elems=localSize, shared=False)
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
			#Use internal memory object to improve perfomance in each worker
			if type(stack[0]) == IMemoryObject and t > 0:
				obj = ISortModule.getIObjectStatic(context, elems=len(stack[0]))
				stack[0].moveTo(obj)
				return obj
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

	def sampling(self, sampleSize, idx, master):
		try:
			logger.info("ISortModule sampling")
			object_in = self._executorData.loadObject()
			pivots = self.getIObject(elems=sampleSize, storage="memory")
			reader = object_in.readIterator()
			writer = pivots.writeIterator()
			size = object_in.getSize()
			div = int((size - sampleSize) / (sampleSize + 1))
			mod = (size - sampleSize) % (sampleSize + 1)

			for i in range(0, sampleSize):
				reader.skip(div + (1 if i < mod else 0))
				writer.write(reader.next())
			self._executorData.getPostBox().newOutMessage(idx, IMessage(master, pivots))
			logger.info("ISortModule sampled")
		except Exception as ex:
			self.raiseRemote(ex)

	def getPivots(self):
		try:
			logger.info("ISortModule getting pivots")
			msgs = self._executorData.getPostBox().popInBox()
			pivots = self.getIObject(elems=len(msgs) * len(msgs), storage="memory")

			for id, msg in msgs.items():
				msg.getObj().moveTo(pivots)
			self._executorData.loadObject(pivots)
			logger.info("ISortModule pivots got")
		except Exception as ex:
			self.raiseRemote(ex)

	def findPivots(self, nodes):
		try:
			logger.info("ISortModule finding pivots")
			pivots = self._executorData.loadObject()
			nodePivots = self.getIObject(elems=len(nodes), storage="memory")
			reader = pivots.readIterator()
			writer = nodePivots.writeIterator()
			size = pivots.getSize()
			np = len(nodes) - 1
			div = int((size - np) / len(nodes))
			mod = (size - np) % len(nodes)

			for i in range(0, np):
				reader.skip(div + (1 if i < mod else 0))
				writer.write(reader.next())

			for i in range(0, len(nodes)):
				self._executorData.getPostBox().newOutMessage(i, IMessage(nodes[i], nodePivots))

			logger.info("ISortModule pivots ready")
		except Exception as ex:
			self.raiseRemote(ex)

	def exchangePartitions(self, idx, nodes):
		try:
			logger.info("ISortModule exchanging partitions")
			msgs = self._executorData.getPostBox().popInBox()
			object_in = self._executorData.loadObject()
			size = len(object_in)
			pivots = list()
			obj_msg = msgs.popitem()[1].getObj()
			reader_msg = obj_msg.readIterator()
			while reader_msg.hasNext():
				pivots.append(reader_msg.next())

			partitions = [self.getIObject() for i in range(0, len(nodes))]
			writers = [partitions[i].writeIterator() for i in range(0, len(nodes))]

			reader = object_in.readIterator()
			for i in range(0, size):
				elem = reader.next()
				init = 0
				end = len(pivots)
				while init < end:
					if elem < pivots[int((end - init) / 2)]:
						end = int((end - init) / 2)
					else:
						init = int((end - init) / 2 + 1)
				writers[init].write(elem)

			for i in range(0, len(nodes)):
				if len(partitions[i]) > 0:
					self._executorData.getPostBox().newOutMessage(idx * len(nodes) + i,
					                                              IMessage(nodes[i], partitions[i]))

			logger.info("ISortModule partitions exchanged")
		except Exception as ex:
			self.raiseRemote(ex)

	def mergePartitions(self):
		try:
			logger.info("ISortModule merging partitions")
			msgs = self._executorData.getPostBox().popInBox()
			object_out = self.getIObject()

			for id, msg in msgs.items():
				msg.getObj().moveTo(object_out)

			self._executorData.loadObject(object_out)
			logger.info("ISortModule partitions merged")
		except Exception as ex:
			self.raiseRemote(ex)
