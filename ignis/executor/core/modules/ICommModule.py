import logging

from ignis.executor.core.modules.IModule import IModule
from ignis.executor.core.modules.impl.ICommImpl import ICommImpl
from ignis.rpc.executor.comm.ICommModule import Iface as ICommModuleIface

logger = logging.getLogger(__name__)


class ICommModule(IModule, ICommModuleIface):

	def __init__(self, executor_data):
		IModule.__init__(self, executor_data, logger)
		self.__impl = ICommImpl(executor_data)

	def createGroup(self):
		try:
			return self.__impl.createGroup()
		except Exception as ex:
			self._pack_exception(ex)

	def joinGroupMembers(self, group, id, size):
		try:
			self.__impl.joinGroupMembers(group, id, size)
		except Exception as ex:
			self._pack_exception(ex)

	def joinToGroup(self, group, id):
		try:
			self.__impl.joinToGroup(group, id)
		except Exception as ex:
			self._pack_exception(ex)

	def hasGroup(self, id):
		try:
			return self.__impl.hasGroup(id)
		except Exception as ex:
			self._pack_exception(ex)

	def destroyGroup(self, id):
		try:
			self.__impl.destroyGroup(id)
		except Exception as ex:
			self._pack_exception(ex)

	def destroyGroups(self):
		try:
			self.__impl.destroyGroups()
		except Exception as ex:
			self._pack_exception(ex)

	def getProtocol(self):
		try:
			return self.__impl.getProtocol()
		except Exception as ex:
			self._pack_exception(ex)

	def getPartitions(self, protocol):
		try:
			return self.__impl.getPartitions(protocol)
		except Exception as ex:
			self._pack_exception(ex)

	def getPartitions2(self, protocol, minPartitions):
		try:
			return self.__impl.getPartitions(protocol, minPartitions)
		except Exception as ex:
			self._pack_exception(ex)

	def setPartitions(self, partitions):
		try:
			self.__impl.setPartitions(partitions)
		except Exception as ex:
			self._pack_exception(ex)

	def setPartitions2(self, partitions, src):
		try:
			self._use_source(src)
			self.__impl.setPartitions(partitions)
		except Exception as ex:
			self._pack_exception(ex)

	def driverGather(self, id, tp):
		try:
			# tp is generated in driver for static type languages
			self.__impl.driverGather(id)
		except Exception as ex:
			self._pack_exception(ex)

	def driverGather0(self, id, tp):
		try:
			# tp is generated in driver for static type languages
			self.__impl.driverGather0(id)
		except Exception as ex:
			self._pack_exception(ex)

	def driverScatter(self, id, partitions):
		try:
			self.__impl.driverScatter(id, partitions)
		except Exception as ex:
			self._pack_exception(ex)

	def driverScatter3(self, id, partitions, src):
		try:
			self._use_source(src)
			self.__impl.driverScatter(id, partitions)
		except Exception as ex:
			self._pack_exception(ex)

	def send(self, id, partition, dest, tag):
		try:
			self.__impl.send(id, partition, dest, tag)
		except Exception as ex:
			self._pack_exception(ex)

	def recv(self, id, partition, source, tag):
		try:
			self.__impl.recv(id, partition, source, tag)
		except Exception as ex:
			self._pack_exception(ex)

	def recv5(self, id, partition, source, tag, src):
		try:
			self._use_source(src)
			self.__impl.recv(id, partition, source, tag)
		except Exception as ex:
			self._pack_exception(ex)
