import unittest
import random
from ignis.executor.core.modules.IShuffleModule import IShuffleModule
from ignis.rpc.executor.shuffle.ttypes import ISplit
from ignis.executor.core.IExecutorData import IExecutorData


class IStorageModuleTest(unittest.TestCase):

	def setUp(self):
		self.__executorData = IExecutorData()
		self.__shuffleModule = IShuffleModule(self.__executorData)
		self.__executorData.getContext()["ignis.executor.storage"] = "raw memory"
		self.__executorData.getContext()["ignis.transport.serialization"] = "ignis"
		self.__executorData.getContext()["ignis.executor.storage.compression"] = "6"

	def tearDown(self):
		pass

	def test_shuffle(self):
		obj = self.__shuffleModule.getIObject()

		random.seed(0)
		input = [random.randint(0, 100) for i in range(0, 1001)]
		writer = obj.writeIterator()
		for elem in input:
			writer.write(elem)

		self.__executorData.loadObject(obj)

		splits = list()
		order = [1, 3, 2, 4]
		splits.append(ISplit(1, "local", len(input) // 4))
		splits.append(ISplit(3, "local", len(input) // 4))
		splits.append(ISplit(2, "local", len(input) // 4))
		splits.append(ISplit(4, "local", len(input) // 4 + len(input) % 4))

		self.__shuffleModule.createSplits(splits)

		msgs = self.__executorData.getPostBox().popOutBox()
		for i in range(0, len(order)):
			self.assertEqual(splits[i].length, msgs[order[i]].getObj().getSize())

		for msg in msgs.items():
			self.__executorData.getPostBox().newInMessage(msg[0], msg[1])

		self.__shuffleModule.joinSplits(order)
		result = self.__executorData.loadObject()

		reader = result.readIterator()
		self.assertEqual(len(input), len(result))
		for elem in input:
			self.assertEqual(elem, reader.next())
