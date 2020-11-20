import gc
import os
import random
import unittest

import numpy

from ignis.executor.core.IExecutorData import IExecutorData
from ignis.executor.core.io.INumpy import disable, enable
from ignis.executor.core.storage import IMemoryPartition, IRawMemoryPartition, IDiskPartition, IPartitionGroup

disable()


class IMpiTest:

	def __init__(self):
		gc.collect()  # Remove old IDiskPartition before reuse the id in other test
		self.__executor_data = IExecutorData()
		props = self.__executor_data.getContext().props()
		props["ignis.transport.compression"] = "6"
		props["ignis.partition.compression"] = "6"
		props["ignis.executor.directory"] = os.getcwd()
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
			self.assertEqual(elems, result)
		self.__executor_data.mpi().barrier()

	def test_gather0(self):
		self.__gather(0)

	def test_gather1(self):
		self.__gather(1)

	def test_bcast(self):
		n = 100
		elems = self._elemens(n)
		part = self._create()
		if self.__executor_data.mpi().isRoot(1):
			self.__insert(elems, part)
		else:
			# Ensures that the partition will be cleaned
			self.__insert([elems[-1]], part)
		self.__executor_data.mpi().bcast(part, 1)
		result = self.__get(part)
		self.assertEqual(elems, result)

		self.__executor_data.mpi().barrier()

	def test_sendRcv(self):
		n = 100
		rank = self.__executor_data.mpi().rank()
		elems = self._elemens(n)
		part = self._create()

		if rank % 2 == 0:
			if rank + 1 < self.__executor_data.mpi().executors():
				self.__executor_data.mpi().recv(part, rank + 1, 0)
				result = self.__get(part)
				self.assertEqual(elems, result)
		else:
			self.__insert(elems, part)
			self.__executor_data.mpi().send(part, rank - 1, 0)

		self.__executor_data.mpi().barrier()

	def __sendRcvGroup(self, partitionType):

		n = 100
		rank = self.__executor_data.mpi().rank()
		elems = self._elemens(n)

		if rank % 2 == 0:
			part = self._create()
			if rank + 1 < self.__executor_data.mpi().executors():
				self.__executor_data.mpi().recvGroup(self.__executor_data.mpi().native(), part, rank + 1, 0)
				result = self.__get(part)
				self.assertEqual(elems, result)
		else:
			part = self._create(partitionType)
			self.__insert(elems, part)
			self.__executor_data.mpi().sendGroup(self.__executor_data.mpi().native(), part, rank - 1, 0)

		self.__executor_data.mpi().barrier()

	def test_sendRcvGroupToMemory(self):
		self.__sendRcvGroup("Memory")

	def test_sendRcvGroupToRawMemory(self):
		self.__sendRcvGroup("RawMemory")

	def test_sendRcvGroupToDisk(self):
		self.__sendRcvGroup("Disk")

	def test_driverGather(self):
		try:
			n = 100
			driver = 0
			rank = self.__executor_data.mpi().rank()
			elems = self._elemens(n)
			part_group = IPartitionGroup()
			if rank != driver:
				part = self._create()
				local_elems = elems[n * (rank - 1): n * rank]
				self.__insert(local_elems, part)
				part_group.add(part)

			self.__executor_data.mpi().driverGather(self.__executor_data.mpi().native(), part_group)

			if rank == driver:
				result = self.__get(part_group[0])
				self.assertEqual(elems, result)

			self.__executor_data.mpi().barrier()
		except Exception as ex:
			import sys
			print("Exception: ", str(ex), file=sys.stderr)
			sys.stderr.flush()

	def test_driverScatter(self):
		n = 100
		driver = 0
		rank = self.__executor_data.mpi().rank()
		size = self.__executor_data.mpi().executors()
		elems = self._elemens(n * (size - 1))
		part_group = IPartitionGroup()
		if rank == driver:
			part = self._create("Memory")  # Scatter is always from user memory array
			self.__insert(elems, part)
			part_group.add(part)

		self.__executor_data.mpi().driverScatter(self.__executor_data.mpi().native(), part_group, (size - 1) * 2)

		if rank != driver:
			local_elems = elems[n * (rank - 1): n * rank]
			result = list()
			for part in part_group:
				result += self.__get(part)
			self.assertEqual(local_elems, result)

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

	def _create(self, partitionType=None):
		if partitionType is None:
			return self.__executor_data.getPartitionTools().newPartition()
		return self.__executor_data.getPartitionTools().newPartition(partitionType)


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

	def setUp(self):
		enable()

	def tearDown(self):
		disable()


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


class IDiskMpiTest(IMpiTest, unittest.TestCase):

	def __init__(self, *args, **kwargs):
		unittest.TestCase.__init__(self, *args, **kwargs)
		IMpiTest.__init__(self)

	def _configure(self, props, vars):
		props["ignis.partition.type"] = IDiskPartition.TYPE
		props["ignis.partition.serialization"] = 'ignis'

	def _elemens(self, n):
		random.seed(0)
		return [random.randint(0, 256) for i in range(0, n)]
