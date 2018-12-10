import unittest
import random
from ignis.executor.core.modules.IStorageModule import IStorageModule
from ignis.executor.core.IExecutorData import IExecutorData
from ignis.data.IBytearrayTransport import IBytearrayTransport


class IStorageModuleTest(unittest.TestCase):

	def setUp(self):
		self.__executorData = IExecutorData()
		self.__storageModule = IStorageModule(self.__executorData)
		self.__executorData.getContext()["ignis.executor.storage"] = "raw memory"
		self.__executorData.getContext()["ignis.transport.serialization"] = "ignis"
		self.__executorData.getContext()["ignis.executor.storage.compression"] = "6"

		random.seed(0)
		self.__input = [random.randint(0, 1000) for i in range(0, 1000)]
		obj = self.__storageModule.getIObject()
		writer = obj.writeIterator()
		for elem in self.__input:
			writer.write(elem)
		self.__executorData.loadObject(obj)

	def tearDown(self):
		pass

	def test_take(self):
		taked = self.__storageModule.take(msg_id=1, addr="", n=100, light=True)
		takedObj = self.__storageModule.getIObject()
		takedObj.read(IBytearrayTransport(taked))

		reader = takedObj.readIterator()
		self.assertEqual(100, len(takedObj))
		for i, elem in zip(range(0, len(takedObj)), self.__input):
			self.assertEqual(elem, reader.next())

	def test_takeSampleWithRemplacement(self):
		sample = self.__storageModule.takeSample(msg_id=1, addr="", n=100, withRemplacement=True, seed=0, light=True)
		sampleObj = self.__storageModule.getIObject()
		sampleObj.read(IBytearrayTransport(sample))

		reader = sampleObj.readIterator()
		self.assertEqual(100, len(sampleObj))
		while reader.hasNext():
			self.assert_(reader.next() in self.__input)

	def test_takeSampleWithoutRemplacement(self):
		sample = self.__storageModule.takeSample(msg_id=1, addr="", n=100, withRemplacement=False, seed=0, light=True)
		sampleObj = self.__storageModule.getIObject()
		sampleObj.read(IBytearrayTransport(sample))

		count = dict()
		for elem in self.__input:
			count[elem] = count.get(elem, 0) + 1

		reader = sampleObj.readIterator()
		self.assertEqual(100, len(sampleObj))
		while reader.hasNext():
			value = reader.next()
			self.assert_(value in count)
			self.assert_(count[value] > 0)
			count[value] -= 1

	def test_collect(self):
		collected = self.__storageModule.collect(msg_id=1, addr="", light=True)
		collectedObj = self.__storageModule.getIObject()
		collectedObj.read(IBytearrayTransport(collected))

		reader = collectedObj.readIterator()
		self.assertEqual(1000, len(collectedObj))
		for elem in self.__input:
			self.assertEqual(elem, reader.next())
