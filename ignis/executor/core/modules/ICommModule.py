from ignis.executor.core.modules.IModule import IModule
from ignis.rpc.executor.comm.ICommModule import Iface as ICommModuleIface, Processor as ICommModuleProcessor
from ignis.executor.core.transport.IBytesTransport import IBytesTransport
from ignis.executor.core.transport.IMemoryBuffer import IMemoryBuffer
from ignis.executor.core.IMpi import MPI
import logging
import ctypes

logger = logging.getLogger(__name__)


class ICommModule(IModule, ICommModuleIface):

	def __init__(self, executor_data):
		IModule.__init__(self, executor_data)
		self.__groups = dict()

	def createGroup(self):
		try:
			logger.info("Comm: creating group")
			port_name = MPI.Open_port()

			class Handle:
				def __del__(self):  # Close port on context clear
					MPI.Close_port(port_name)

			logger.info("Comm: group created on " + str(port_name))
			self._executor_data.setVariable("server", Handle())
			return port_name
		except Exception as ex:
			self._pack_exception(ex)

	def joinGroupMembers(self, group_name, id, size):
		try:
			logger.info(
				"Comm: member id " + str(id) + " preparing to join group " + group_name + ", total " + str(size))
			group = MPI.COMM_SELF
			flag = ctypes.c_bool(True)
			if id == 0:
				client_comms = [MPI.COMM_NULL for i in range(size)]

				class Handle:
					def __del__(self):
						for comm in client_comms:
							if comm != MPI.COMM_NULL:
								comm.Free()

				handle = Handle()

				for i in range(0, size):
					client_comm = MPI.COMM_SELF.Accept(group_name, MPI.INFO_NULL, 0)
					pos = ctypes.c_longlong(0)
					client_comm.Recv((pos, 1, MPI.LONG_LONG_INT), 0, 1963)
					logger.info("Comm: member " + str(id) + " found")
					client_comms[pos.value] = client_comm
				logger.info("Comm: all members found")

				for i in range(0, size):
					client_comm = client_comms[i]
					client_comm.Send((flag, 1, MPI.BOOL), 0, 1963)
					info_comm = group.Accept(group_name, MPI.INFO_NULL, 0)
					group = self.__addComm(group, client_comm, MPI.COMM_SELF, group != MPI.COMM_SELF)
					logger.info("Comm: new member added to the group")
					client_comm.Free()
					client_comms[i] = MPI.COMM_NULL
					info_comm.Free()

				logger.info("Comm: all members added to the group")

			else:
				group = MPI.COMM_NULL
				logger.info("Comm: connecting to the group " + group_name)
				client_comm = MPI.COMM_SELF.Connect(group_name, MPI.INFO_NULL, 0)
				logger.info("Comm: connected to the group, waiting for my turn")
				pos = ctypes.c_longlong(id)
				client_comm.Send((pos, 1, MPI.LONG_LONG_INT), 0, 1963)
				client_comm.Recv((flag, 1, MPI.BOOL), 0, 1963)
				info_comm = MPI.COMM_SELF.Connect(group_name.c_str(), MPI.INFO_NULL, 0)
				group = self.__addComm(group, client_comm, MPI.COMM_SELF, False)
				logger.info("Comm: joined to the group")
				client_comm.Free()
				info_comm.Free()
				client_comm = MPI.COMM_NULL

				for i in range(id + 1, size):
					group.Accept(group_name, MPI.INFO_NULL, 0).Free()
					logger.info("Comm: new member added to the group")
					group = self.__addComm(group, client_comm, MPI.COMM_SELF, True)

			self._executor_data.getContext()._mpi_group = group
			logger.info("Comm: group ready with " + str(group.Get_size()) + " members")
		except Exception as ex:
			self._pack_exception(ex)

	def joinToGroup(self, group_name, id):
		try:
			group = self._executor_data.getContext().mpiGroup()
			permanent = group == MPI.COMM_SELF or group == MPI.COMM_WORLD
			new_group = group
			member = self._executor_data.hasVariable("server")
			group.Bcast((ctypes.c_bool(member), 1, MPI.BOOL), 0)
			if self._executor_data.getContext().executorId() == 0:
				if member:
					info_comm = group.Accept(group_name, MPI.INFO_NULL, 0)
					client_comm = MPI.COMM_SELF.Accept(group_name.c_str(), MPI.INFO_NULL, 0)
					new_group = self.__addComm(group, client_comm, MPI.COMM_SELF, not permanent)
					client_comm.Free()
					info_comm.Free()
				else:
					new_group = MPI.COMM_NULL
					info_comm = group.Connect(group_name, MPI.INFO_NULL, 0)
					client_comm = MPI.COMM_SELF.Connect(group_name, MPI.INFO_NULL, 0)
					new_group = self.__addComm(new_group, client_comm, MPI.COMM_SELF, not permanent)
					client_comm.Free()
					info_comm.Free()
			else:
				client_comm = MPI.COMM_NULL
				if member:
					new_group = MPI.COMM_NULL
				group.Accept(group_name, MPI.INFO_NULL, 0).Free()
				new_group = self.__addComm(new_group, client_comm, MPI.COMM_SELF, False)

			self.__groups[id] = new_group
		except Exception as ex:
			self._pack_exception(ex)

	def __addComm(self, group, comm, local, detroyGroup):
		peer = MPI.COMM_NULL
		up = group == MPI.COMM_NULL

		if comm != MPI.COMM_NULL:
			peer = comm.Merge(up)

		if up:#new member
			new_comm = local.Create_intercomm(0, peer, 0, 1963)
		else:
			new_comm = group.Create_intercomm(0, peer, 1, 1963)

		new_group = new_comm.Merge(up)

		if peer != MPI.COMM_NULL:
			peer.Free()
		new_comm.Free()

		if detroyGroup:
			group.Free()

		return new_group

	def hasGroup(self, id):
		try:
			return id in self.__groups.keys()
		except Exception as ex:
			self._pack_exception(ex)

	def destroyGroup(self, id):
		try:
			group = self.__groups.get(id, None)
			if group:
				group.Free()
				del self.__groups[id]

		except Exception as ex:
			self._pack_exception(ex)

	def destroyGroups(self):
		try:
			for key, group in self.__groups.items():
				group.Free()
			self.__groups.clear()
		except Exception as ex:
			self._pack_exception(ex)

	def getPartitions(self):
		try:
			partitions = list()
			group = self._executor_data.getPartitions()
			cmp = self._executor_data.getProperties().msgCompression()
			buffer = IMemoryBuffer()
			for part in group:
				buffer.resetBuffer()
				part.write(part, cmp)
				partitions.append(buffer.getBufferAsBytes())
			return partitions
		except Exception as ex:
			self._pack_exception(ex)

	def setPartitions(self, partitions):
		try:
			group = self._executor_data.getPartitionTools().newPartitionGroup(len(partitions))
			for i in range(0, len(partitions)):
				group[i].read(IBytesTransport(partitions[i]))
			self._executor_data.setPartitions(group)
		except Exception as ex:
			self._pack_exception(ex)

	def setPartitions2(self, partitions, src):
		try:
			self._use_source(src)
		except Exception as ex:
			self._pack_exception(ex)
		self.setPartitions(partitions)

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
