from ignis.executor.core.modules.IModule import IModule
from ignis.rpc.executor.general.action.IGeneralActionModule import Iface as IGeneralActionModuleIface, \
	Processor as IGeneralActionModuleProcessor


class IGeneralActionModule(IModule, IGeneralActionModuleIface):

	def __init__(self, executor_data):
		IModule.__init__(self, executor_data)

	def reduce(self, src):
		try:
			raise NotImplementedError()
		except Exception as ex:
			self._pack_exception(ex)

	def treeReduce(self, src, depth):
		try:
			raise NotImplementedError()
		except Exception as ex:
			self._pack_exception(ex)

	def collect(self):
		try:
			raise NotImplementedError()
		except Exception as ex:
			self._pack_exception(ex)

	def aggregate(self, seqOp, combOp):
		try:
			raise NotImplementedError()
		except Exception as ex:
			self._pack_exception(ex)

	def treeAggregate(self, seqOp, combOp, depth):
		try:
			raise NotImplementedError()
		except Exception as ex:
			self._pack_exception(ex)

	def fold(self, src):
		try:
			raise NotImplementedError()
		except Exception as ex:
			self._pack_exception(ex)

	def take(self, num):
		try:
			raise NotImplementedError()
		except Exception as ex:
			self._pack_exception(ex)

	def foreach_(self, src):
		try:
			raise NotImplementedError()
		except Exception as ex:
			self._pack_exception(ex)

	def foreachPartition(self, src):
		try:
			raise NotImplementedError()
		except Exception as ex:
			self._pack_exception(ex)

	def top(self, num):
		try:
			raise NotImplementedError()
		except Exception as ex:
			self._pack_exception(ex)

	def top2(self, num, cmp):
		try:
			raise NotImplementedError()
		except Exception as ex:
			self._pack_exception(ex)

