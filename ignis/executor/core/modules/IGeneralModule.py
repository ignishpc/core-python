from ignis.executor.core.modules.IModule import IModule
from ignis.rpc.executor.general.IGeneralModule import Iface as IGeneralModuleIface, Processor as IGeneralModuleProcessor
from ignis.executor.core.modules.impl.IPipeImpl import IPipeImpl
from ignis.executor.core.modules.impl.ISortImpl import ISortImpl


class IGeneralModule(IModule, IGeneralModuleIface):

	def __init__(self, executor_data):
		IModule.__init__(self, executor_data)
		self.__pipe_impl = IPipeImpl(executor_data)
		self.__sort_impl = ISortImpl(executor_data)

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

	def mapPartitions(self, src, preservesPartitioning):
		try:
			self.__pipe_impl.mapPartitions(self._executor_data.loadLibrary(src), preservesPartitioning)
		except Exception as ex:
			self._pack_exception(ex)

	def mapPartitionsWithIndex(self, src, preservesPartitioning):
		try:
			self.__pipe_impl.mapPartitionsWithIndex(self._executor_data.loadLibrary(src), preservesPartitioning)
		except Exception as ex:
			self._pack_exception(ex)

	def applyPartition(self, src):
		try:
			self.__pipe_impl.applyPartition(self._executor_data.loadLibrary(src))
		except Exception as ex:
			self._pack_exception(ex)

	def groupBy(self, src):
		try:
			raise NotImplementedError()
		except Exception as ex:
			self._pack_exception(ex)

	def groupBy2(self, src, numPartitions):
		try:
			raise NotImplementedError()
		except Exception as ex:
			self._pack_exception(ex)

	def sort(self, ascending):
		try:
			self.__sort_impl.sort(ascending, -1)
		except Exception as ex:
			self._pack_exception(ex)

	def sort2(self, ascending, numPartitions):
		try:
			self.__sort_impl.sort(ascending, numPartitions)
		except Exception as ex:
			self._pack_exception(ex)

	def sortBy(self, src, ascending):
		try:
			self.__sort_impl.sort(self._executor_data.loadLibrary(src), ascending, -1)
		except Exception as ex:
			self._pack_exception(ex)

	def sortBy3(self, src, ascending, numPartitions):
		try:
			self.__sort_impl.sort(self._executor_data.loadLibrary(src), ascending, numPartitions)
		except Exception as ex:
			self._pack_exception(ex)
