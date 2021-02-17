import math
import random
import unittest

from ignis.executor.core.modules.IIOModule import IIOModule
from ignis_test.executor.core.modules.IModuleTest import IModuleTest


class IIOModuleTest(IModuleTest, unittest.TestCase):

	def __init__(self, *args, **kwargs):
		IModuleTest.__init__(self)
		unittest.TestCase.__init__(self, *args, **kwargs)
		self.__io = IIOModule(self._executor_data)
		props = self._executor_data.getContext().props()
		props["ignis.partition.minimal"] = "10MB"
		props["ignis.partition.type"] = "Memory"
		props["ignis.modules.io.overwrite"] = "true"

	def test_textFile1(self):
		self.__textFileTest(1)

	def test_textFileN(self):
		self.__textFileTest(8)

	def test_saveAsTextFile(self):
		self.__saveAsTextFileTest(8)

	# -------------------------------------Impl-------------------------------------

	def __textFileTest(self, n):
		random.seed(0)
		alphanum = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
		path = "./tmpfile.txt"
		executors = self._executor_data.getContext().executors()
		lines = list()
		with open(path, "w") as file:
			for l in range(10000):
				lc = random.randint(0, 100)
				line = ""
				for l in range(lc):
					line += alphanum[random.randint(0, len(alphanum) - 1)]
				file.write(line)
				file.write('\n')
				lines.append(line)

		self.__io.textFile2(path, n)

		self.assertGreaterEqual(math.ceil(n / executors), self.__io.partitionCount())

		result = self.getFromPartitions()

		self.loadToPartitions(result, 1)
		self._executor_data.mpi().gather(self._executor_data.getPartitions()[0], 0)
		result = self.getFromPartitions()

		if self._executor_data.mpi().isRoot(0):
			self.assertEqual(lines, result)

	def __saveAsTextFileTest(self, n):
		random.seed(0)
		alphanum = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
		path = "./tmpfile.txt"
		id = self._executor_data.getContext().executorId()
		lines = list()
		with open(path, "w") as file:
			for l in range(10000):
				lc = random.randint(0, 100)
				line = ""
				for l in range(lc):
					line += alphanum[random.randint(0, len(alphanum) - 1)]
				file.write(line)
				lines.append(line)

		self.loadToPartitions(lines, n)
		self.__io.saveAsTextFile("./tmpsave", id)
