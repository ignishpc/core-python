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

	def Dtest_mapper(self):
		self.__executorData.getContext()["ignis.executor.cores"] = "1"
		path = os.path.abspath(__file__)
		dir = os.path.dirname(path)
		sf = ISource(name=dir + "/TestFunctions.py:MapFunction")
		self.__mapperModule._map(sf)

		reader = self.__executorData.loadObject().readIterator()
		for elem in self.__input:
			self.assertEqual(str(elem), reader.next())
