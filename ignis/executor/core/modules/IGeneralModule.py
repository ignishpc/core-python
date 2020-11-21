import logging

from ignis.executor.core.modules.IModule import IModule
from ignis.executor.core.modules.impl.IPipeImpl import IPipeImpl
from ignis.executor.core.modules.impl.IReduceImpl import IReduceImpl
from ignis.executor.core.modules.impl.ISortImpl import ISortImpl
from ignis.rpc.executor.general.IGeneralModule import Iface as IGeneralModuleIface

logger = logging.getLogger(__name__)


class IGeneralModule(IModule, IGeneralModuleIface):

	def __init__(self, executor_data):
		IModule.__init__(self, executor_data, logger)
		self.__pipe_impl = IPipeImpl(executor_data)
		self.__sort_impl = ISortImpl(executor_data)
		self.__reduce_impl = IReduceImpl(executor_data)

	def map_(self, src):
		try:
			self.__pipe_impl.map(self._executor_data.loadLibrary(src))
		except Exception as ex:
			self._pack_exception(ex)

	def filter(self, src):
		try:
			self.__pipe_impl.filter(self._executor_data.loadLibrary(src))
		except Exception as ex:
			self._pack_exception(ex)

	def flatmap(self, src):
		try:
			self.__pipe_impl.flatmap(self._executor_data.loadLibrary(src))
		except Exception as ex:
			self._pack_exception(ex)

	def keyBy(self, src):
		try:
			raise NotImplementedError()  # TODO
		except Exception as ex:
			self._pack_exception(ex)

	def mapPartitions(self, src):
		try:
			raise NotImplementedError()  # TODO self.__pipe_impl.mapPartitions(self._executor_data.loadLibrary(src), preservesPartitioning)
		except Exception as ex:
			self._pack_exception(ex)

	def mapPartitionsWithIndex(self, src, preservesPartitioning):
		try:
			self.__pipe_impl.mapPartitionsWithIndex(self._executor_data.loadLibrary(src), preservesPartitioning)
		except Exception as ex:
			self._pack_exception(ex)

	def mapExecutor(self, src):
		try:
			raise NotImplementedError()  # TODO self.__pipe_impl.applyPartition(self._executor_data.loadLibrary(src))
		except Exception as ex:
			self._pack_exception(ex)

	def mapExecutorTo(self, src):
		try:
			raise NotImplementedError()  # TODO
		except Exception as ex:
			self._pack_exception(ex)

	def groupBy(self, src, numPartitions):
		try:
			raise NotImplementedError()
		except Exception as ex:
			self._pack_exception(ex)

	def sort(self, ascending):
		try:
			self.__sort_impl.sort(ascending)
		except Exception as ex:
			self._pack_exception(ex)

	def sort2(self, ascending, numPartitions):
		try:
			self.__sort_impl.sort(ascending, numPartitions)
		except Exception as ex:
			self._pack_exception(ex)

	def sortBy(self, src, ascending):
		try:
			self.__sort_impl.sortBy(self._executor_data.loadLibrary(src), ascending)
		except Exception as ex:
			self._pack_exception(ex)

	def sortBy3(self, src, ascending, numPartitions):
		try:
			self.__sort_impl.sortBy(self._executor_data.loadLibrary(src), ascending, numPartitions)
		except Exception as ex:
			self._pack_exception(ex)

	def flatMapValues(self, src):
		try:
			raise NotImplementedError()  # TODO
		except Exception as ex:
			self._pack_exception(ex)

	def mapValues(self, src):
		try:
			raise NotImplementedError()  # TODO
		except Exception as ex:
			self._pack_exception(ex)

	def groupByKey(self, numPartitions):
		try:
			raise NotImplementedError()  # TODO
		except Exception as ex:
			self._pack_exception(ex)

	def groupByKey2(self, numPartitions, src):
		try:
			raise NotImplementedError()  # TODO
		except Exception as ex:
			self._pack_exception(ex)

	def reduceByKey(self, src, numPartitions, localReduce):
		try:
			raise NotImplementedError()  # TODO
		except Exception as ex:
			self._pack_exception(ex)

	def aggregateByKey(self, zero, seqOp, numPartitions):
		try:
			raise NotImplementedError()  # TODO
		except Exception as ex:
			self._pack_exception(ex)

	def aggregateByKey4(self, zero, seqOp, combOp, numPartitions):
		try:
			raise NotImplementedError()  # TODO
		except Exception as ex:
			self._pack_exception(ex)

	def foldByKey(self, zero, src, numPartitions, localFold):
		try:
			raise NotImplementedError()  # TODO
		except Exception as ex:
			self._pack_exception(ex)

	def sortByKey(self, ascending):
		try:
			raise NotImplementedError()  # TODO
		except Exception as ex:
			self._pack_exception(ex)

	def sortByKey2a(self, ascending, numPartitions):
		try:
			raise NotImplementedError()  # TODO
		except Exception as ex:
			self._pack_exception(ex)

	def sortByKey2b(self, src, ascending):
		try:
			raise NotImplementedError()  # TODO
		except Exception as ex:
			self._pack_exception(ex)

	def sortByKey3(self, src, ascending, numPartitions):
		try:
			raise NotImplementedError()  # TODO
		except Exception as ex:
			self._pack_exception(ex)
