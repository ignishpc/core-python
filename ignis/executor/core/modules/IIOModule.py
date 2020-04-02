from ignis.executor.core.modules.IModule import IModule
from ignis.rpc.executor.io.IIOModule import Iface as IIOModuleIface, Processor as IIOModuleProcessor


class IIOModule(IModule, IIOModuleIface):

	def __init__(self, executor_data):
		IModule.__init__(self, executor_data)

	def partitionCount(self):
		try:
			raise NotImplementedError()
		except Exception as ex:
			self._pack_exception(ex)

	def partitionApproxSize(self):
		try:
			raise NotImplementedError()
		except Exception as ex:
			self._pack_exception(ex)

	def textFile(self, path):
		try:
			raise NotImplementedError()
		except Exception as ex:
			self._pack_exception(ex)

	def textFile2(self, path, minPartitions):
		try:
			raise NotImplementedError()
		except Exception as ex:
			self._pack_exception(ex)

	def partitionObjectFile(self, path, first, partitions):
		try:
			raise NotImplementedError()
		except Exception as ex:
			self._pack_exception(ex)

	def partitionObjectFile4(self, path, first, partitions, src):
		try:
			raise NotImplementedError()
		except Exception as ex:
			self._pack_exception(ex)

	def partitionTextFile(self, path, first, partitions):
		try:
			raise NotImplementedError()
		except Exception as ex:
			self._pack_exception(ex)

	def partitionJsonFile4a(self, path, first, partitions, objectMapping):
		try:
			raise NotImplementedError()
		except Exception as ex:
			self._pack_exception(ex)

	def partitionJsonFile4b(self, path, first, partitions, src):
		try:
			raise NotImplementedError()
		except Exception as ex:
			self._pack_exception(ex)

	def saveAsObjectFile(self, path, compression, first):
		try:
			raise NotImplementedError()
		except Exception as ex:
			self._pack_exception(ex)

	def saveAsTextFile(self, path, first):
		try:
			raise NotImplementedError()
		except Exception as ex:
			self._pack_exception(ex)

	def saveAsJsonFile(self, path, first, pretty):
		try:
			raise NotImplementedError()
		except Exception as ex:
			self._pack_exception(ex)

