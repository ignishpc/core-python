import unittest
import random
from ignis.executor.core.IExecutorData import IExecutorData
from ignis.executor.core.modules.ISortModule import ISortModule


class ISortModuleTest(unittest.TestCase):

	def setUp(self):
		self.__executorData = IExecutorData()
		self.__sortModule = ISortModule(self.__executorData)

		self.__executorData.getContext()["ignis.executor.storage"] = "rawMemory"
		self.__executorData.getContext()["ignis.executor.cores"] = "1"
		self.__executorData.getContext()["ignis.transport.serialization"] = "ignis"
		self.__executorData.getContext()["ignis.executor.storage.compression"] = "6"

	def tearDown(self):
		pass

	def test_sequentialLocalSort(self):
		self.__executorData.getContext()["ignis.executor.cores"] = "1"
		self.__localSort()

	def test_parallelLocalSort(self):
		self.__executorData.getContext()["ignis.executor.cores"] = "8"
		self.__localSort()

	def __localSort(self):
		random.seed(0)
		input = [random.randint(0, 100) for i in range(0, 100)]
		obj = self.__sortModule.getIObject(shared=True)
		writer = obj.writeIterator()
		for elem in input:
			writer.write(elem)

		self.__executorData.loadObject(obj)
		self.__sortModule.localSort(True)

		result = self.__executorData.loadObject()
		self.assertEqual(len(input), len(result))
		reader = result.readIterator()
		last = reader.next()
		while reader.hasNext():
			next = reader.next()
			self.assertLessEqual(last, next)
			last = next

	def test_sort(self):
		random.seed(0)
		executors = 3
		input = [[random.randint(0, 100) for i in range(0, 10 + e)] for e in range(0, executors)]
		object = [self.__sortModule.getIObject(shared=True) for e in range(0, executors)]
		writer = [object[e].writeIterator() for e in range(0, executors)]
		msgs = [None for e in range(0, executors)]
		nodes = [str(e) for e in range(0, executors)]

		for e in range(0, executors):
			for elem in input[e]:
				writer[e].write(elem)

		for e in range(0, executors):
			self.__executorData.loadObject(object[e])
			self.__sortModule.localSort(True)
			object[e] = self.__executorData.loadObject()

		for e in range(0, executors):
			self.__executorData.loadObject(object[e])
			self.__sortModule.sampling(executors, e, "addr0")
			msgs[e] = self.__executorData.getPostBox().popOutBox()
			self.assertEqual(executors, len(msgs[e][e].getObj()))

		for e in range(0, executors):
			self.__executorData.getPostBox().newInMessage(e, msgs[e][e])

		self.__sortModule.getPivots()
		self.assertEqual(executors * executors, len(self.__executorData.loadObject()))
		self.__sortModule.localSort(True)
		self.__sortModule.findPivots(nodes)

		msgs_master = self.__executorData.getPostBox().popOutBox()
		self.assertEqual(executors - 1, len(list(msgs_master.values())[0].getObj()))

		for e in range(0, executors):
			self.__executorData.getPostBox().newInMessage(e, msgs_master[e])
			self.__executorData.loadObject(object[e])
			self.__sortModule.exchangePartitions(e, nodes)
			msgs[e] = self.__executorData.getPostBox().popOutBox()

		for e in range(0, executors):
			self.__executorData.loadObject(object[e]);
			for msgs_ex in msgs:
				for msg in msgs_ex.items():
					if msg[1].getAddr() == nodes[e]:
						self.__executorData.getPostBox().newInMessage(msg[0], msg[1])
			self.__sortModule.mergePartitions()
			self.__sortModule.localSort(True)
			self.__executorData.loadObject()

		zero = 0
		for e in range(0, executors):
			zero += len(input[e]) - len(object[e])
		self.assertEqual(0, zero)

		last = object[0].readIterator().next()
		for e in range(0, executors):
			reader = object[0].readIterator()
			while reader.hasNext():
				next = reader.next()
				self.assertLessEqual(last, next)

