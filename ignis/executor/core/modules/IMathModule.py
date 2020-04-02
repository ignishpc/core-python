from ignis.executor.core.modules.IModule import IModule
from ignis.rpc.executor.math.IMathModule import Iface as IMathModuleIface, Processor as IMathModuleProcessor


class IMathModule(IModule, IMathModuleIface):

	def __init__(self, executor_data):
		IModule.__init__(self, executor_data)

	def sample(self, withReplacement, fraction, seed):
		raise NotImplementedError()

	def takeSample(self, withReplacement, fraction, seed):
		raise NotImplementedError()

	def count(self):
		try:
			raise NotImplementedError()
		except Exception as ex:
			self._pack_exception(ex)

	def max(self, cmp):
		try:
			raise NotImplementedError()
		except Exception as ex:
			self._pack_exception(ex)

	def min(self, cmp):
		try:
			raise NotImplementedError()
		except Exception as ex:
			self._pack_exception(ex)
