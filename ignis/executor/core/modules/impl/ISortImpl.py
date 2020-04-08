from ignis.executor.core.modules.impl.IBaseImpl import IBaseImpl
from ignis.executor.core.IMpi import MPI
import logging
import math
import functools

logger = logging.getLogger(__name__)


class ISortImpl(IBaseImpl):

	def sort(self, ascending, numPartitions):
		self.__sortImpl(None, ascending, numPartitions)

	def sortBy(self, f, ascending, numPartitions):
		context = self._executor_data.getContext()
		f.before(context)
		self.__sortImpl(lambda a, b: f.call(a, b, context), ascending, numPartitions)
		f.after(context)

	def __sortImpl(self, cmp, ascending, partitions):
		input = self._executor_data.getPartitions()
		executors = self._executor_data.mpi().executors()
		# Copy the data if they are reused
		if input.cache():  # Work directly on the array to improve performance
			if self._executor_data.getPartitionTools().isMemory(input):
				input = input.clone()
			else:
				# Only group will be affected
				input = input.shadowCopy()
		# Sort each partition
		logger.info("Sort: sorting " + str(len(input)) + " partitions locally")
		self.__localSort(input, cmp, ascending)

		localPartitions = input.partitions()
		totalPartitions = self._executor_data.mpi().native().allreduce(localPartitions, MPI.SUM)
		if totalPartitions < 2:
			self._executor_data.setPartitions(input)
			return

		# Generates pivots to separate the elements in order
		samples = self._executor_data.getProperties().sortSamples()
		if partitions > 0:
			samples *= int(partitions / len(input) + 1)
		logger.info("Sort: selecting " + str(samples) + " pivots")
		pivots = self.__selectPivots(input, samples)

		logger.info("Sort: collecting pivots")
		self._executor_data.mpi().gather(pivots, 0)

		if self._executor_data.mpi().isRoot(0):
			group = self._executor_data.getPartitionTools().newPartitionGroup(0)
			group.add(pivots)
			self.__localSort(group, cmp, ascending)
			if partitions > 0:
				samples = partitions - 1
			else:
				samples = totalPartitions - 1

			logger.info("Sort: selecting " + str(samples) + " partition pivots")
			pivots = self.__selectPivots(group, samples)

		logger.info("Sort: broadcasting pivots ranges")
		self._executor_data.mpi().bcast(pivots, 0)

		ranges = self.__generateRanges(input, pivots, cmp)
		output = self._executor_data.getPartitionTools().newPartitionGroup()
		executor_ranges = math.ceil(len(ranges) / executors)
		target = -1
		logger.info("Sort: exchanging ranges")
		for p in range(len(ranges)):
			if p % executor_ranges == 0:
				target += 1
			self._executor_data.mpi().gather(ranges[p], target)
			if self._executor_data.mpi().isRoot(target):
				output.add(ranges[p])
			else:
				ranges[p] = None

		# Sort final partitions
		logger.info("Sort: sorting again " + str(len(output)) + " partitions locally")
		self.__localSort(output, cmp, ascending)
		self._executor_data.setPartitions(output)

	def __localSort(self, group, cmp, ascending):
		inMemory = self._executor_data.getPartitionTools().isMemory(group)

		for i in range(0, len(group)):
			part = group[i]
			if not inMemory:
				new_part = self._executor_data.getPartitionTools().newMemoryPartition(part.size())
				part.moveTo(new_part)
				part = new_part

			elems = part._IMemoryPartition__elements

			if isinstance(elems, bytearray):
				if cmp:
					part._IMemoryPartition__elements = bytearray(
						sorted(elems, key=functools.cmp_to_key(cmp), reverse=not ascending))
				else:
					part._IMemoryPartition__elements = bytearray(sorted(elems, reverse=not ascending))
			elif type(elems).__name__ == 'INumpyWrapperCl':
				if cmp:
					raise TypeError("ndarray does not support sortBy")
				else:
					if ascending:
						elems.array.sort()
					else:
						elems.array[::-1].sort()
			else:
				if cmp:
					elems.sort(key=functools.cmp_to_key(cmp), reverse=not ascending)
				else:
					elems.sort(reverse=not ascending)

			if not inMemory:
				part.moveTo(group[i])

	def __selectPivots(self, group, samples):
		pivots = self._executor_data.getPartitionTools().newMemoryPartition()
		inMemory = self._executor_data.getPartitionTools().isMemory(group)
		writer = pivots.writeIterator()
		for part in group:
			skip = int((len(part) - samples) / (samples + 1))
			if inMemory:
				pos = skip
				for n in range(0, samples):
					writer.write(part[pos])
					pos += skip + 1
			else:
				reader = part.readIterator()
				for n in range(0, samples):
					for i in range(skip):
						reader.next()
					writer.write(reader.next())

		return pivots

	def __generateRanges(self, input, pivots, cmp):
		ranges = self._executor_data.getPartitionTools().newPartitionGroup(len(pivots) + 1)
		writers = map(lambda p: p.writeIterator(), ranges)

		for part in input:
			for elem in part:
				writers[self.__searchRange(elem, pivots, cmp)].write(elem)
			part.clear()

		input.clear()
		return ranges

	def __searchRange(self, elem, pivots, cmp):
		start = 0
		end = len(pivots) - 1
		while start < end:
			mid = int((start + end) / 2)
			if cmp(elem, pivots[mid]):
				end = mid - 1
			else:
				start = mid + 1
		if cmp(elem, pivots[start]):
			return start
		else:
			return start + 1
