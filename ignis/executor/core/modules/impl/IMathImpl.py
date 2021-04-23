import logging
import math
import random

from ignis.executor.core.modules.impl.IBaseImpl import IBaseImpl

logger = logging.getLogger(__name__)


class IMathImpl(IBaseImpl):

	def __init__(self, executor_data):
		IBaseImpl.__init__(self, executor_data, logger)

	def sample(self, withReplacement, num, seed):
		input = self._executor_data.getAndDeletePartitions()
		output = self._executor_data.getPartitionTools().newPartitionGroup(1)

		logger.info("Math: sample " + str(len(input)) + " partitions")
		writer = output[0].writeIterator()
		random.seed(seed)
		for p in range(len(input)):
			part = input[p]
			sz = len(sz)
			if self._executor_data.getPartitionTools().isMemory(part):
				aux = self._executor_data.getPartitionTools().newMemoryPartition(sz)
				part.copyTo(aux)
				part = aux

			if withReplacement:
				for i in range(num[p]):
					for j in range(num[p]):
						prob = num[p] / (sz - j)
						rand = random.uniform(0, 1)
						if rand < prob:
							writer.write(part[j])
							break
			else:
				picked = 0
				for i in range(num[p]):
					prob = (num[p] - picked) / (sz - i)
					rand = random.uniform(0, 1)
					if rand < prob:
						writer.write(part[j])
						picked += 1
			del input[p]

		self._executor_data.setPartitions(output)

	def count(self):
		n = 0
		input = self._executor_data.getPartitions()
		logger.info("Math: count " + str(len(input)) + " partitions")
		for part in input:
			n += len(part)
		self._executor_data.deletePartitions()
		return n

	def sampleByKey(self, withReplacement, fractions, seed):
		raise NotImplementedError()#TODO

	def countByKey(self):
		input = self._executor_data.getPartitions()
		logger.info("Math: counting local keys " + str(len(input)) + " partitions")

		acum = dict()
		for part in input:
			for key, _ in part:
				if key in acum:
					acum[key] += 1
				else:
					acum[key] = 1

		self.__countByReduce(acum)

	def countByValue(self):
		input = self._executor_data.getPartitions()
		logger.info("Math: counting local value " + str(len(input)) + " partitions")

		acum = dict()
		for part in input:
			for _, value in part:
				if value in acum:
					acum[value] += 1
				else:
					acum[value] = 1

		self.__countByReduce(acum)

	def __countByReduce(self, acum):
		logger.info("Math: reducing global counting")
		elem_part = self._executor_data.getPartitionTools().newMemoryPartition()
		rank = self._executor_data.mpi().rank()
		pivotUp = self._executor_data.mpi().executors()
		while pivotUp > 1:
			pivotDown = math.floor(pivotUp / 2)
			pivotUp = math.ceil(pivotUp / 2)
			if rank < pivotDown:
				self._executor_data.mpi().recv(elem_part, rank + pivotUp, 0)
				for elem, count in elem_part:
					if elem in acum:
						acum[elem] += count
					else:
						acum[elem] = count
			elif rank >= pivotUp:
				writer = elem_part.writeIterator()
				for pair in acum.items():
					writer.write(pair)
				acum.clear()
				self._executor_data.mpi().send(elem_part, rank - pivotUp, 0)
			elem_part.clear()

		output = self._executor_data.getPartitionTools().newPartitionGroup()
		if self._executor_data.mpi().isRoot(0):
			writer = elem_part.writeIterator()
			for pair in acum.items():
				writer.write(pair)
			acum.clear()
			output.add(elem_part)

		self._executor_data.setPartitions(output)
