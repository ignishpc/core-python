import unittest
import random
from ignis.executor.core.storage import IMemoryPartition, IRawMemoryPartition, IDiskPartition, IPartitionGroup
from ignis.executor.core.IExecutorData import IExecutorData
import numpy
import os


class IMpiTest:

	def __init__(self):
		self.__disk_id = 0
		self.__executor_data = IExecutorData()
		props = self.__executor_data.getContext().props()
		props["ignis.transport.compression"] = "6"
		props["ignis.partition.compression"] = "6"
		vars = self.__executor_data.getContext().vars()
		self._configure(props, vars)

	def _configure(self, props, vars):
		pass

	def _elemens(self, n):
		raise NotImplementedError()

	def __gather(self, root):
		n = 10
		rank = self.__executor_data.mpi().rank()
		size = self.__executor_data.mpi().executors()
		elems = self._elemens(size * n)
		local_elem = elems[rank * n:(rank + 1) * n]
		part = self._create()
		self.__insert(local_elem, part)
		self.__executor_data.mpi().gather(part, root)
		if self.__executor_data.mpi().isRoot(root):
			result = self.__get(part)
			self.assert_(elems == result)
		self.__executor_data.mpi().barrier()

	def test_gather0(self):
		self.__gather(0)

	def _test_gather1(self):
		self.__gather(1)

	def test_bcast(self):
		elems = self._elemens(100)
		part = self._create()
		if self.__executor_data.mpi().isRoot(1):
			self.__insert(elems, part)
		else:
			# Ensures that the partition will be cleaned
			self.__insert([elems[-1]], part)
		self.__executor_data.mpi().bcast(part, 1)
		result = self.__get(part)
		self.assert_(elems == result)
		self.__executor_data.mpi().barrier()

	def __insert(self, elems, part):
		it = part.writeIterator()
		for elem in elems:
			it.write(elem)

	def __get(self, part):
		l = list()
		it = part.readIterator()
		while it.hasNext():
			l.append(it.next())
		return l

	def _create(self):
		return self.__executor_data.getPartitionTools().newPartition()

"""
class IMemoryBytesMpiTest(IMpiTest, unittest.TestCase):

	def __init__(self, *args, **kwargs):
		unittest.TestCase.__init__(self, *args, **kwargs)
		IMpiTest.__init__(self)

	def _configure(self, props, vars):
		props["ignis.partition.type"] = IMemoryPartition.TYPE
		props["ignis.partition.serialization"] = 'ignis'
		vars["STORAGE_CLASS"] = bytearray

	def _elemens(self, n):
		random.seed(0)
		return [random.randint(0, 256) for i in range(0, n)]


class IMemoryNumpyMpiTest(IMpiTest, unittest.TestCase):

	def __init__(self, *args, **kwargs):
		unittest.TestCase.__init__(self, *args, **kwargs)
		IMpiTest.__init__(self)

	def _configure(self, props, vars):
		props["ignis.partition.type"] = IMemoryPartition.TYPE
		props["ignis.partition.serialization"] = 'ignis'
		vars["STORAGE_CLASS"] = numpy.ndarray
		vars['STORAGE_CLASS_DTYPE'] = int

	def _elemens(self, n):
		random.seed(0)
		return [random.randint(0, 256) for i in range(0, n)]


class IMemoryDefaultMpiTest(IMpiTest, unittest.TestCase):

	def __init__(self, *args, **kwargs):
		unittest.TestCase.__init__(self, *args, **kwargs)
		IMpiTest.__init__(self)

	def _configure(self, props, vars):
		props["ignis.partition.type"] = IMemoryPartition.TYPE
		props["ignis.partition.serialization"] = 'ignis'

	def _elemens(self, n):
		random.seed(0)
		return [random.randint(0, 256) for i in range(0, n)]

class IRawMemoryMpiTest(IMpiTest, unittest.TestCase):

	def __init__(self, *args, **kwargs):
		unittest.TestCase.__init__(self, *args, **kwargs)
		IMpiTest.__init__(self)

	def _configure(self, props, vars):
		props["ignis.partition.type"] = IRawMemoryPartition.TYPE
		props["ignis.partition.serialization"] = 'ignis'

	def _elemens(self, n):
		random.seed(0)
		return [random.randint(0, 256) for i in range(0, n)]

"""
class IDiskMpiTest(IMpiTest, unittest.TestCase):

	def __init__(self, *args, **kwargs):
		unittest.TestCase.__init__(self, *args, **kwargs)
		IMpiTest.__init__(self)

	def _configure(self, props, vars):
		props["ignis.partition.type"] = IDiskPartition.TYPE
		props["ignis.partition.serialization"] = 'ignis'
		props["ignis.job.directory"] = os.getcwd()

	def _elemens(self, n):
		random.seed(0)
		return [random.randint(0, 256) for i in range(0, n)]
