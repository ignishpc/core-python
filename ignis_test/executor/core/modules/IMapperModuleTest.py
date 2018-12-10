import unittest
import random
import os
from ignis.executor.core.modules.IMapperModule import IMapperModule
from ignis.executor.core.IExecutorData import IExecutorData
from ignis.rpc.source.ttypes import ISource


class IMapperModuleTest(unittest.TestCase):

	def setUp(self):
		self.__executorData = IExecutorData()
		self.__mapperModule = IMapperModule(self.__executorData)

		self.__executorData.getContext()["ignis.executor.storage"] = "raw memory"
		self.__executorData.getContext()["ignis.transport.serialization"] = "ignis"
		self.__executorData.getContext()["ignis.executor.storage.compression"] = "6"
		self.__executorData.getContext()["ignis.executor.cores.chunk"] = "5"

		random.seed(0)
		self.__input = [random.randint(0, 100) for i in range(0, 100)]
		obj = self.__mapperModule.getIObject()
		writer = obj.writeIterator()
		for elem in self.__input:
			writer.write(elem)
		self.__executorData.loadObject(obj)

	def tearDown(self):
		pass

	def test_sequentialMap(self):
		self.__executorData.getContext()["ignis.executor.cores"] = "1"
		path = os.path.abspath(__file__)
		dir = os.path.dirname(path)
		sf = ISource(name=dir + "/TestFunctions.py:MapFunction")
		self.__mapperModule._map(sf)

		reader = self.__executorData.loadObject().readIterator()
		for elem in self.__input:
			self.assertEqual(str(elem), reader.next())

	def test_sequentialFlatmap(self):
		self.__executorData.getContext()["ignis.executor.cores"] = "1"
		path = os.path.abspath(__file__)
		dir = os.path.dirname(path)
		sf = ISource(name=dir + "/TestFunctions.py:FlatmapFunction")
		self.__mapperModule.flatmap(sf)

		reader = self.__executorData.loadObject().readIterator()
		for elem in self.__input:
			if elem % 2 == 0:
				self.assertEqual(str(elem), reader.next())

	def test_sequentialFilter(self):
		self.__executorData.getContext()["ignis.executor.cores"] = "1"
		path = os.path.abspath(__file__)
		dir = os.path.dirname(path)
		sf = ISource(name=dir + "/TestFunctions.py:FilterFunction")
		self.__mapperModule.filter(sf)

		reader = self.__executorData.loadObject().readIterator()
		for elem in self.__input:
			if elem % 2 == 0:
				self.assertEqual(elem, reader.next())

	def test_sequentialKeyBy(self):
		self.__executorData.getContext()["ignis.executor.cores"] = "1"
		path = os.path.abspath(__file__)
		dir = os.path.dirname(path)
		sf = ISource(name=dir + "/TestFunctions.py:MapFunction")
		self.__mapperModule.keyBy(sf)

		reader = self.__executorData.loadObject().readIterator()
		for elem in self.__input:
			tuple = reader.next()
			self.assertEqual(elem, tuple[1])
			self.assertEqual(str(elem), tuple[0])

	def test_parallelMap(self):
		self.__executorData.getContext()["ignis.executor.cores"] = "8"
		path = os.path.abspath(__file__)
		dir = os.path.dirname(path)
		sf = ISource(name=dir + "/TestFunctions.py:MapFunction")
		self.__mapperModule._map(sf)

		reader = self.__executorData.loadObject().readIterator()
		for elem in self.__input:
			self.assertEqual(str(elem), reader.next())

	def test_parallelFlatmap(self):
		self.__executorData.getContext()["ignis.executor.cores"] = "8"
		path = os.path.abspath(__file__)
		dir = os.path.dirname(path)
		sf = ISource(name=dir + "/TestFunctions.py:FlatmapFunction")
		self.__mapperModule.flatmap(sf)

		reader = self.__executorData.loadObject().readIterator()
		for elem in self.__input:
			if elem % 2 == 0:
				self.assertEqual(str(elem), reader.next())

	def test_parallelFilter(self):
		self.__executorData.getContext()["ignis.executor.cores"] = "8"
		path = os.path.abspath(__file__)
		dir = os.path.dirname(path)
		sf = ISource(name=dir + "/TestFunctions.py:FilterFunction")
		self.__mapperModule.filter(sf)

		reader = self.__executorData.loadObject().readIterator()
		for elem in self.__input:
			if elem % 2 == 0:
				self.assertEqual(elem, reader.next())

	def test_parallelKeyBy(self):
		self.__executorData.getContext()["ignis.executor.cores"] = "8"
		path = os.path.abspath(__file__)
		dir = os.path.dirname(path)
		sf = ISource(name=dir + "/TestFunctions.py:MapFunction")
		self.__mapperModule.keyBy(sf)

		reader = self.__executorData.loadObject().readIterator()
		for elem in self.__input:
			tuple = reader.next()
			self.assertEqual(elem, tuple[1])
			self.assertEqual(str(elem), tuple[0])

	def test_values(self):
		obj = self.__mapperModule.getIObject()
		writer = obj.writeIterator()
		for elem in self.__input:
			writer.write((elem, elem))
		self.__executorData.loadObject(obj)
		self.__mapperModule.values()

		reader = self.__executorData.loadObject().readIterator()
		for elem in self.__input:
			self.assertEqual(elem, reader.next())