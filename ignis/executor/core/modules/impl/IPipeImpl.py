import logging

from ignis.executor.core.modules.impl.IBaseImpl import IBaseImpl

logger = logging.getLogger(__name__)


class IPipeImpl(IBaseImpl):

	def __init__(self, executor_data):
		IBaseImpl.__init__(self, executor_data)

	def map(self, f):
		context = self._executor_data.getContext()
		input = self._executor_data.getPartitions()
		f.before(context)
		output = self._executor_data.getPartitionTools().newPartitionGroup(input)
		logger.info("General: map " + str(len(input)) + " partitions")
		for i in range(len(input)):
			it = output[i].writeIterator()
			for elem in input[i]:
				it.write(f.call(elem, context))
		f.after(context)
		self._executor_data.setPartitions(output)

	def filter(self, f):
		context = self._executor_data.getContext()
		input = self._executor_data.getPartitions()
		f.before(context)
		output = self._executor_data.getPartitionTools().newPartitionGroup(input)
		logger.info("General: filter " + str(len(input)) + " partitions")
		for i in range(len(input)):
			it = output[i].writeIterator()
			for elem in input[i]:
				if f.call(elem, context):
					it.write(elem)
		f.after(context)
		self._executor_data.setPartitions(output)

	def flatmap(self, f):
		context = self._executor_data.getContext()
		input = self._executor_data.getPartitions()
		f.before(context)
		output = self._executor_data.getPartitionTools().newPartitionGroup(input)
		logger.info("General: flatmap " + str(len(input)) + " partitions")
		for i in range(len(input)):
			it = output[i].writeIterator()
			for elem in input[i]:
				for elem2 in f.call(elem, context):
					it.write(elem2)
		f.after(context)
		self._executor_data.setPartitions(output)

	def mapPartitions(self, f, preservesPartitioning):
		context = self._executor_data.getContext()
		input = self._executor_data.getPartitions()
		f.before(context)
		output = self._executor_data.getPartitionTools().newPartitionGroup(input)
		logger.info("General: flatmap " + str(len(input)) + " partitions")
		for i in range(len(input)):
			it = output[i].writeIterator()
			for elem in f.call(input[i].readIterator(), context):
				it.write(elem)
		f.after(context)
		self._executor_data.setPartitions(output)

	def mapPartitionsWithIndex(self, f, preservesPartitioning):
		context = self._executor_data.getContext()
		input = self._executor_data.getPartitions()
		f.before(context)
		output = self._executor_data.getPartitionTools().newPartitionGroup(input)
		logger.info("General: flatmap " + str(len(input)) + " partitions")
		for i in range(len(input)):
			it = output[i].writeIterator()
			for elem in f.call(i, input[i].readIterator(), context):
				it.write(elem)
		f.after(context)
		self._executor_data.setPartitions(output)

	def applyPartition(self, f):
		raise NotImplementedError()
