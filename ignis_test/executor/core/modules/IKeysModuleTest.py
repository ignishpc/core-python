import unittest
import random
import os
from ignis.executor.core.modules.IKeysModule import IKeysModule
from ignis.executor.core.IExecutorData import IExecutorData
from ignis.rpc.source.ttypes import ISource
from ignis.rpc.executor.keys.ttypes import IExecutorKeys
from ignis.executor.core.IMessage import IMessage


class IKeysModuleTest(unittest.TestCase):

	def setUp(self):
		self.__executorData = IExecutorData()
		self.__keysModule = IKeysModule(self.__executorData)

		self.__executorData.getContext()["ignis.executor.storage"] = "raw memory"
		self.__executorData.getContext()["ignis.transport.serialization"] = "ignis"
		self.__executorData.getContext()["ignis.executor.storage.compression"] = "6"

	def tearDown(self):
		pass

	def __reduceByKeyTest(self):
		obj = self.__keysModule.getIObject()
		path = os.path.abspath(__file__)
		dir = os.path.dirname(path)
		sf = ISource(name=dir + "/TestFunctions.py:ReduceByKeyFunction")

		reduction = dict()
		writer = obj.writeIterator()
		random.seed(0)
		rands = [random.randint(0, 10) for i in range(0, 100)]
		for elem in rands:
			value = (elem, 1)
			reduction[value[0]] = reduction.get(value[0], 0) + value[1]
			writer.write(value)

		self.__executorData.loadObject(obj)

		self.__keysModule.reduceByKey(sf)

		obj = self.__executorData.loadObject()
		reader = obj.readIterator()

		self.assertEqual(len(reduction), obj.getSize())
		while reader.hasNext():
			value = reader.next()
			self.assert_(value[0] in reduction)
			self.assertEqual(reduction[value[0]], value[1])

		hashes = self.__keysModule.getKeys()
		self.assertEqual(len(reduction), len(hashes))

		executorKeys = [IExecutorKeys(keys=list()) for i in range(0, 4)]
		reduction2 = dict()
		object_rcva = self.__keysModule.getIObject()
		object_rcvb = self.__keysModule.getIObject()
		writer_rcva = object_rcva.writeIterator()
		writer_rcvb = object_rcvb.writeIterator()

		reader = obj.readIterator()
		for i in range(0, len(hashes)):
			value = reader.next()
			if hashes[i] % 4 < 2:
				if hashes[i] % 2 == 1:
					executorKeys[0].keys.append(hashes[i])
					reduction2[value[0]] = reduction[value[0]]
					if random.randint(0, 1) == 1:
						value2 = (value[0], i + 1)
						writer_rcva.write(value2)
						reduction2[value[0]] += value2[1]
					else:
						value2 = (value[0], i + 1)
						writer_rcvb.write(value2)
						reduction2[value[0]] += value2[1]
				else:
					executorKeys[1].keys.append(hashes[i])
			else:
				if hashes[i] % 2 == 1:
					executorKeys[2].keys.append(hashes[i])
				else:
					executorKeys[3].keys.append(hashes[i])
		for i in range(0, len(executorKeys)):
			executorKeys[i].msg_id = i
			executorKeys[i].addr = "ip" + str(i + 1)
		executorKeys[0].addr = "local"

		self.__keysModule.prepareKeys(executorKeys)

		for id, msg in self.__executorData.getPostBox().popOutBox().items():
			if msg.getAddr() == "local":
				self.__executorData.getPostBox().newInMessage(id, msg)
		self.__executorData.getPostBox().newInMessage(4, IMessage(addr="local", obj=object_rcva))
		self.__executorData.getPostBox().newInMessage(5, IMessage(addr="local", obj=object_rcvb))

		self.__keysModule.collectKeys()

		self.__keysModule.reduceByKey(sf)

		obj = self.__executorData.loadObject()
		reader = obj.readIterator()

		self.assertEqual(len(reduction2), obj.getSize())
		while reader.hasNext():
			value = reader.next()
			self.assert_(value[0] in reduction2)
			self.assertEqual(reduction2[value[0]], value[1])

	def test_secuencialReduceByKeyTest(self):
		self.__executorData.getContext()["ignis.executor.cores"] = "1"
		self.__reduceByKeyTest()

	def test_parallelReduceByKeyTest(self):
		self.__executorData.getContext()["ignis.executor.cores"] = "8"
		self.__reduceByKeyTest()
