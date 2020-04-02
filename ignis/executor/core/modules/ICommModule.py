from ignis.executor.core.modules.IModule import IModule
from ignis.rpc.executor.comm.ICommModule import Iface as ICommModuleIface, Processor as ICommModuleProcessor


class ICommModule(IModule, ICommModuleIface):

	def __init__(self, executor_data):
		IModule.__init__(self, executor_data)

	def createGroup(self):
		try:
			raise NotImplementedError()
		except Exception as ex:
			self._pack_exception(ex)

	def joinGroupMembers(self, group, id, size):
		try:
			raise NotImplementedError()
		except Exception as ex:
			self._pack_exception(ex)

	def joinToGroup(self, group, id):
		try:
			raise NotImplementedError()
		except Exception as ex:
			self._pack_exception(ex)

	def hasGroup(self, id):
		try:
			raise NotImplementedError()
		except Exception as ex:
			self._pack_exception(ex)

	def destroyGroup(self, id):
		try:
			raise NotImplementedError()
		except Exception as ex:
			self._pack_exception(ex)

	def destroyGroups(self):
		try:
			raise NotImplementedError()
		except Exception as ex:
			self._pack_exception(ex)

	def getPartitions(self):
		try:
			raise NotImplementedError()
		except Exception as ex:
			self._pack_exception(ex)

	def setPartitions(self, partitions):
		try:
			raise NotImplementedError()
		except Exception as ex:
			self._pack_exception(ex)

	def setPartitions2(self, partitions, src):
		try:
			raise NotImplementedError()
		except Exception as ex:
			self._pack_exception(ex)

	def driverGather(self, id, src):
		try:
			raise NotImplementedError()
		except Exception as ex:
			self._pack_exception(ex)

	def driverGather0(self, id, src):
		try:
			raise NotImplementedError()
		except Exception as ex:
			self._pack_exception(ex)

	def driverScatter(self, id, dataId):
		try:
			raise NotImplementedError()
		except Exception as ex:
			self._pack_exception(ex)

	def driverScatter3(self, id, dataId, src):
		try:
			raise NotImplementedError()
		except Exception as ex:
			self._pack_exception(ex)

