import logging

from ignis.executor.core.modules.IModule import IModule
from ignis.executor.core.modules.impl.IMathImpl import IMathImpl
from ignis.executor.core.modules.impl.ISortImpl import ISortImpl
from ignis.rpc.executor.math.IMathModule import Iface as IMathModuleIface

logger = logging.getLogger(__name__)


class IMathModule(IModule, IMathModuleIface):

	def __init__(self, executor_data):
		IModule.__init__(self, executor_data, logger)
		self.__math_impl = IMathImpl(executor_data)
		self.__sort_impl = ISortImpl(executor_data)

	def sample(self, withReplacement, num, seed):
		try:
			raise NotImplementedError()
		except Exception as ex:
			self._pack_exception(ex)

	def count(self):
		try:
			raise NotImplementedError()
		except Exception as ex:
			self._pack_exception(ex)

	def max(self):
		try:
			raise NotImplementedError()
		except Exception as ex:
			self._pack_exception(ex)

	def min(self):
		try:
			raise NotImplementedError()
		except Exception as ex:
			self._pack_exception(ex)

	def max1(self, cmp):
		try:
			raise NotImplementedError()
		except Exception as ex:
			self._pack_exception(ex)

	def min1(self, cmp):
		try:
			raise NotImplementedError()
		except Exception as ex:
			self._pack_exception(ex)

	def sampleByKey(self, withReplacement, fractions, seed):
		try:
			raise NotImplementedError()
		except Exception as ex:
			self._pack_exception(ex)

	def countByKey(self):
		try:
			raise NotImplementedError()
		except Exception as ex:
			self._pack_exception(ex)

	def countByValue(self):
		try:
			raise NotImplementedError()
		except Exception as ex:
			self._pack_exception(ex)
