import unittest
import random
from ignis_test.executor.core.storage.IPartitionTest import IPartitionTest
from ignis.executor.core.storage.IRawMemoryPartition import IRawMemoryPartition


class IRawMemoryPartitionTest(IPartitionTest, unittest.TestCase):

	def create(self):
		return IRawMemoryPartition(native=False, bytes=1000, compression=6)

	def elemens(self, n):
		random.seed(0)
		return [random.randint(0, n) for i in range(0, n)]


class IRawMemoryPartitionPairTest(IPartitionTest, unittest.TestCase):

	def create(self):
		return IRawMemoryPartition(native=False, bytes=1000, compression=6)

	def elemens(self, n):
		random.seed(0)
		return [(str(random.randint(0, n)), random.randint(0, n)) for i in range(0, n)]


class IRawMemoryPartitionNativeTest(IPartitionTest, unittest.TestCase):

	def create(self):
		return IRawMemoryPartition(native=True, bytes=1000, compression=6)

	def elemens(self, n):
		return [random.randint(0, n) for i in range(0, n)]
