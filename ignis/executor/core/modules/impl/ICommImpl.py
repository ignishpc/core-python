import logging

from ignis.executor.core.IMpi import MPI
from ignis.executor.core.modules.impl.IBaseImpl import IBaseImpl
from ignis.executor.core.protocol.IObjectProtocol import IObjectProtocol
from ignis.executor.core.storage import IMemoryPartition
from ignis.executor.core.transport.IBytesTransport import IBytesTransport
from ignis.executor.core.transport.IMemoryBuffer import IMemoryBuffer
from ignis.executor.core.transport.IZlibTransport import IZlibTransport

logger = logging.getLogger(__name__)


class ICommImpl(IBaseImpl):

	def __init__(self, executor_data):
		IBaseImpl.__init__(self, executor_data, logger)
		self.__groups = dict()

	def openGroup(self):
		logger.info("Comm: creating group")
		port_name = MPI.Open_port()

		class Handle:
			def __del__(self):  # Close port on context clear
				MPI.Close_port(port_name)

		logger.info("Comm: group created on " + str(port_name))
		self._executor_data.setVariable("server", Handle())
		return port_name

	def closeGroup(self):
		logger.info("Comm: closing group server")
		self._executor_data.removeVariable("server")

	def joinToGroup(self, id, leader):
		root = self._executor_data.hasVariable("server")
		comm = self._executor_data.mpi().native()
		if root:
			peer = MPI.COMM_SELF.Accept(id)
		else:
			peer = comm.Connect(id)
		comm = self.__addComm(comm, peer, leader, comm != MPI.COMM_WORLD)
		self._executor_data.setMpiGroup(comm)

	def joinToGroupName(self, id, leader, name):
		root = self._executor_data.hasVariable("server")
		comm = self._executor_data.mpi().native()
		if root:
			peer = MPI.COMM_SELF.Accept(id)
		else:
			peer = comm.Connect(id)
		comm = self.__addComm(comm, peer, leader, comm != MPI.COMM_WORLD)
		self.__groups[name] = comm

	def hasGroup(self, name):
		return name in self.__groups[name]

	def destroyGroup(self, name):
		if len(name) == 0:
			comm = self._executor_data.mpi().native()
			comm.Free()
			self._executor_data.setMpiGroup(MPI.COMM_WORLD)
		elif name in self.__groups[name]:
			comm = self.__groups[name]
			comm.Free()
			del self.__groups[name]

	def destroyGroups(self):
		for name, comm in self.__groups:
			comm.Free
		self.__groups.clear()
		self.destroyGroup("")

	def getProtocol(self):
		return IObjectProtocol.PYTHON_PROTOCOL

	def getPartitions(self, protocol, minPartitions=1):
		partitions = list()
		group = self._executor_data.getPartitions()
		cmp = self._executor_data.getProperties().msgCompression()
		native = self.getProtocol() == protocol and self._executor_data.getProperties().nativeSerialization()
		buffer = IMemoryBuffer()
		if len(group) > minPartitions:
			for part in group:
				buffer.resetBuffer()
				part.write(part, cmp, native)
				partitions.append(buffer.getBufferAsBytes())
		elif len(group) == 1 and self._executor_data.getPartitionTools().isMemory(
				group) and protocol == self.getProtocol():
			men = group[0]
			zlib = IZlibTransport(buffer, cmp)
			proto = IObjectProtocol(zlib)
			partition_elems = int(len(men) / minPartitions)
			remainder = len(men) % minPartitions
			offset = 0
			for p in range(minPartitions):
				sz = partition_elems + (1 if p < remainder else 0)
				proto.writeObject(men._IMemoryPartition__elements[offset:offset + sz], native)
				offset += sz
				zlib.flush()
				zlib.reset()
				partitions.append(buffer.getBufferAsBytes())
				buffer.resetBuffer()

		elif len(group) > 0:
			elemens = 0
			for part in group:
				elemens += len(part)
			part = IMemoryPartition(1024 * 1024)
			writer = part.writeIterator()
			partition_elems = int(elemens / minPartitions)
			remainder = elemens % minPartitions
			i = 0
			ew = 0
			er = 0
			it = group[0].readIterator()
			for p in range(minPartitions):
				part.clear()
				ew = partition_elems
				if p < remainder:
					ew += 1

				while ew > 0 and i < len(group):
					if er == 0:
						er = len(group[i])
						it = group[i].readIterator()
						i += 1
					while ew > 0 and er > 0:
						writer.write(it.next())
						ew -= 1
						er -= 1
					part.write(buffer, cmp, native)
					partitions.append(buffer.getBufferAsBytes())
					buffer.resetBuffer()

		return partitions

	def setPartitions(self, partitions):
		group = self._executor_data.getPartitionTools().newPartitionGroup(len(partitions))
		for i in range(0, len(partitions)):
			group[i].read(IBytesTransport(partitions[i]))
		self._executor_data.setPartitions(group)

	def newEmptyPartitions(self, n):
		self._executor_data.setPartitions(self._executor_data.getPartitionTools().newPartitionGroup(n))

	def driverGather(self, group):
		comm = self.__getGroup(group)
		if comm.Get_rank() == 0:
			self._executor_data.setPartitions(self._executor_data.getPartitionTools().newPartitionGroup())
		self._executor_data.mpi().driverGather(comm, self._executor_data.getPartitions())

	def driverGather0(self, group):
		comm = self.__getGroup(group)
		if comm.Get_rank() == 0:
			self._executor_data.setPartitions(self._executor_data.getPartitionTools().newPartitionGroup())
		self._executor_data.mpi().driverGather0(comm, self._executor_data.getPartitions())

	def driverScatter(self, group, partitions):
		comm = self.__getGroup(id)
		if comm.Get_rank() != 0:
			self._executor_data.setPartitions(self._executor_data.getPartitionTools().newPartitionGroup())
		self._executor_data.mpi().driverScatter(comm, self._executor_data.getPartitions(), partitions)

	def enableMultithreading(self, group):
		return 1

	def send(self, group, partition, dest, thread):
		part_group = self._executor_data.getPartitions()
		comm = self.__getGroup(id)
		self._executor_data.mpi().send(comm, part_group[partition], dest)

	def recv(self, group, partition, source, thread):
		part_group = self._executor_data.getPartitions()
		comm = self.__getGroup(id)
		tag = comm.Get_rank()
		self._executor_data.mpi().recv(comm, part_group[partition], source, tag)

	def __addComm(self, group, comm, leader, detroyGroup):
		peer = MPI.COMM_NULL

		if comm != MPI.COMM_NULL:
			peer = comm.Merge(not leader)

		new_comm = group.Create_intercomm(0, peer, 1 if leader else 0, 1963)

		new_group = new_comm.Merge(not leader)

		if comm != MPI.COMM_NULL:
			peer.Free()
		new_comm.Free()

		if detroyGroup:
			group.Free()
		return new_group

	def __getGroup(self, id):
		if id not in self.__groups:
			raise KeyError("Group " + id + " not found")
		return self.__groups[id]
