import logging

from ignis.executor.core.modules.impl.IBaseImpl import IBaseImpl

logger = logging.getLogger(__name__)


class IRepartitionImpl(IBaseImpl):

	def __init__(self, executor_data):
		IBaseImpl.__init__(self, executor_data, logger)

	def repartition(self, numPartitions, preserveOrdering, global_):
		if not global_:
			self.__local_repartition(numPartitions)
		elif preserveOrdering:
			self.__ordered_repartition(numPartitions)
		else:
			self.__unordered_repartition(numPartitions)

	def __ordered_repartition(self, numPartitions):
		input = self._executor_data.getAndDeletePartitions()
		executors = self._executor_data.getContext().executors()
		rank = self._executor_data.mpi().rank()
		executors_count = list()
		local_offset = list()
		local_count = 0
		global_count = 0
		global_offset = 0
		local_offset.append(0)
		for part in input:
			local_count += len(part)
			local_offset.append(len(part))
		executors_count = self._executor_data.mpi().native().allgather(local_count)
		for i in range(executors):
			if i < rank:
				global_offset += executors_count[i]
			global_count += executors_count[i]
		new_partitions_count = list()
		new_partitions_offset = list()
		new_partitions_first = -1

		block = int(global_count / numPartitions)
		remainder = global_count % numPartitions
		new_partitions_offset.append(0)
		for i in range(len(input)):
			new_partitions_count[i] = block
			if i < remainder:
				new_partitions_count[i] += 1
			new_partitions_offset.append(new_partitions_offset[-1] + new_partitions_count[i])
			if global_offset > new_partitions_offset[i] and new_partitions_first == -1:
				new_partitions_first = i

		logger.info("Repartition: ordered repartition from " + str(len(input)) + " partitions")
		global_group = self._executor_data.getPartitionTools().newPartitionGroup(numPartitions)

		for p in range(len(input)):
			reader = input[p].readIterator()
			for i in range(new_partitions_first, len(input)):
				if global_offset + local_offset[p] > new_partitions_offset[i]:
					elems = new_partitions_count[i] - (global_offset + local_offset[p])
					part_offset = i
					writer = global_group[part_offset].writeIterator()
			while reader.hasNext():
				while reader.hasNext() and elems > 0:
					elems -= 1
					writer.write((rank, reader.next()))
				if reader.hasNext():
					part_offset += 1
					elems = new_partitions_count[part_offset]
					writer = global_group[part_offset].writeIterator()
			input[p] = None

		logger.info("Repartition: exchanging new partitions")
		tmp = self._executor_data.getPartitionTools().newPartitionGroup()
		self.exchange(global_group, tmp)
		output = self._executor_data.getPartitionTools().newPartitionGroup()

		for p in range(len(tmp)):
			part = tmp[p]
			if part.empty():
				continue
			new_part = self._executor_data.getPartitionTools().newPartition()
			writer = new_part.writeIterator()
			men_part = self._executor_data.getPartitionTools().newMemoryPartition()
			part.moveTo(men_part)
			first = [0] * executors
			for i in range(len(men_part)):
				first[men_part[i][0]] = i
			for e in first:
				for i in range(first[e], len(men_part)):
					if men_part[i][0] == e:
						writer.write(men_part[i][1])
					else:
						break
			tmp[p] = None
			output.add(new_part)

		self._executor_data.setPartitions(output)

	def __unordered_repartition(self, numPartitions):
		raise NotImplemented()  # TODO

	def __local_repartition(self, numPartitions):
		raise NotImplemented()  # TODO

	def partitionByRandom(self, numPartitions):
		raise NotImplemented()  # TODO

	def partitionByHash(self, numPartitions):
		raise NotImplemented()  # TODO

	def partitionBy(self, f, numPartitions):
		raise NotImplemented()  # TODO
