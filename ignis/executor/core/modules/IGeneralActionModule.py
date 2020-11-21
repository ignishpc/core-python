import logging

from ignis.executor.core.modules.IModule import IModule
from ignis.executor.core.modules.impl.IPipeImpl import IPipeImpl
from ignis.executor.core.modules.impl.IReduceImpl import IReduceImpl
from ignis.executor.core.modules.impl.ISortImpl import ISortImpl
from ignis.rpc.executor.general.action.IGeneralActionModule import Iface as IGeneralActionModuleIface

logger = logging.getLogger(__name__)


class IGeneralActionModule(IModule, IGeneralActionModuleIface):

	def __init__(self, executor_data):
		IModule.__init__(self, executor_data, logger)
		self.__pipe_impl = IPipeImpl(executor_data)
		self.__sort_impl = ISortImpl(executor_data)
		self.__reduce_impl = IReduceImpl(executor_data)

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

	def treeFold(self, src):
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

	def top2(self, num, comp):
		try:
			raise NotImplementedError()
		except Exception as ex:
			self._pack_exception(ex)

	def takeOrdered(self, num):
		try:
			raise NotImplementedError()
		except Exception as ex:
			self._pack_exception(ex)

	def takeOrdered2(self, num, comp):
		try:
			raise NotImplementedError()
		except Exception as ex:
			self._pack_exception(ex)

	def keys(self):
		try:
			raise NotImplementedError()
		except Exception as ex:
			self._pack_exception(ex)

	def values(self):
		try:
			raise NotImplementedError()
		except Exception as ex:
			self._pack_exception(ex)
