from ignis.executor.core.modules.IModule import IModule
from ignis.rpc.executor.general.IGeneralModule import Iface as IGeneralModuleIface, Processor as IGeneralModuleProcessor


class IGeneralModule(IModule, IGeneralModuleIface):

	def __init__(self, executor_data):
		IModule.__init__(self, executor_data)

	def map_(self, src):
		try:
			raise NotImplementedError()
		except Exception as ex:
			self._pack_exception(ex)

	def filter(self, src):
		try:
			raise NotImplementedError()
		except Exception as ex:
			self._pack_exception(ex)

	def flatmap(self, src):
		try:
			raise NotImplementedError()
		except Exception as ex:
			self._pack_exception(ex)

	def mapPartitions(self, src, preservesPartitioning):
		try:
			raise NotImplementedError()
		except Exception as ex:
			self._pack_exception(ex)

	def mapPartitionsWithIndex(self, src, preservesPartitioning):
		try:
			raise NotImplementedError()
		except Exception as ex:
			self._pack_exception(ex)

	def applyPartition(self, src):
		try:
			raise NotImplementedError()
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
			raise NotImplementedError()
		except Exception as ex:
			self._pack_exception(ex)

	def sort2(self, ascending, numPartitions):
		try:
			raise NotImplementedError()
		except Exception as ex:
			self._pack_exception(ex)

	def sortBy(self, src, ascending):
		try:
			raise NotImplementedError()
		except Exception as ex:
			self._pack_exception(ex)

	def sortBy3(self, src, ascending, numPartitions):
		try:
			raise NotImplementedError()
		except Exception as ex:
			self._pack_exception(ex)

