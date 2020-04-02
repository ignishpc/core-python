from ignis.executor.core.modules.IModule import IModule
from ignis.rpc.executor.cachecontext.ICacheContextModule import Iface as ICacheContextModuleIface, \
	Processor as ICacheContextModuleProcessor


class ICacheContextModule(IModule, ICacheContextModuleIface):

	def __init__(self, executor_data):
		IModule.__init__(self, executor_data)

	def saveContext(self):
		try:
			raise NotImplementedError()
		except Exception as ex:
			self._pack_exception(ex)

	def loadContext(self, id):
		try:
			raise NotImplementedError()
		except Exception as ex:
			self._pack_exception(ex)

	def cache(self, id, level):
		try:
			raise NotImplementedError()
		except Exception as ex:
			self._pack_exception(ex)

	def loadCache(self, id):
		try:
			raise NotImplementedError()
		except Exception as ex:
			self._pack_exception(ex)

