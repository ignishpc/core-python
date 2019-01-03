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
		self.__input = [random.randint(0, 100) for i in range(0, 100)]
		obj = self.__sortModule.getIObject(shared=True)
		writer = obj.writeIterator()
		for elem in self.__input:
			writer.write(elem)

		self.__executorData.loadObject(obj)
		self.__sortModule.localSort(True)

		result = self.__executorData.loadObject()
		self.assertEqual(len(self.__input), len(result))
		reader = result.readIterator()
		last = reader.next()
		while reader.hasNext():
			next = reader.next()
			self.assertLessEqual(last, next)
			last = next
