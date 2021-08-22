import logging
from ctypes import c_longlong, c_bool

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
		self._executor_data.setMpiGroup(self.__joinToGroupImpl(id, leader))

	def joinToGroupName(self, id, leader, name):
		self.__groups[name] = self.__joinToGroupImpl(id, leader)

	def hasGroup(self, name):
		return name in self.__groups

	def destroyGroup(self, name):
		if len(name) == 0:
			comm = self._executor_data.mpi().native()
			if comm != MPI.COMM_WORLD:
				comm.Free()
				self._executor_data.setMpiGroup(MPI.COMM_WORLD)
		elif name in self.__groups:
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
			partition_elems = int(elemens / minPartitions)
			remainder = elemens % minPartitions
			i = 0
			ew = 0
			er = 0
			it = group[0].readIterator()
			for p in range(minPartitions):
				part.clear()
				writer = part.writeIterator()
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
		comm = self.__getGroup(group)
		if comm.Get_rank() != 0:
			self._executor_data.setPartitions(self._executor_data.getPartitionTools().newPartitionGroup())
		self._executor_data.mpi().driverScatter(comm, self._executor_data.getPartitions(), partitions)

	def importData(self, group, source, threads):
		comm = self.__getGroup(group)
		executors = comm.Get_size()
		me = comm.Get_size()
		ranges = list()
		queue = list()
		offset = self.__importDataAux(comm, source, ranges, queue)
		if source:
			logger.info("General: importData sending partitions")
		else:
			logger.info("General: importData receiving partitions")

		shared = self._executor_data.getAndDeletePartitions() if source else \
			self._executor_data.getPartitionTools().newPartitionGroup(ranges[me][1] - ranges[me][0])

		for i in range(len(queue)):
			other = queue[i]
			ignore = c_bool(True)
			if other == executors:
				continue
			if source:
				for j in range(ranges[other][0], ranges[other][1]):
					ignore.value = ignore.value and shared[j - offset].empty()
				comm.Send(ignore, 1, MPI.BOOL, other, 0)
			else:
				comm.Recv(ignore, 1, MPI.BOOL, other, 0)
			if ignore:
				continue
			if source:
				first = ranges[other][0]
				its = ranges[other][1] - ranges[other][0]
			else:
				first = ranges[me][0]
				its = ranges[me][1] - ranges[me][0]
			opt = self._executor_data.mpi().getMsgOpt(comm, shared[first].type(), source, other, 0)
			for j in range(its):
				if source:
					self._executor_data.mpi().sendGroup(comm, shared[first - offset + j], other, 0, opt)
					shared[first - offset + j] = None
				else:
					self._executor_data.mpi().recvGroup(comm, shared[first - offset + j], other, 0, opt)

		self._executor_data.setPartitions(shared)

	def __importDataAux(self, group, source, ranges, queue):
		rank = group.Get_rank()
		local_rank = self._executor_data.mpi().rank()
		executors = group.Get_size()
		local_executors = self._executor_data.getContext().executors()
		remote_executors = executors - local_executors
		local_root = rank == 0 if self._executor_data.mpi().rank() else remote_executors
		remote_root = remote_executors if local_root == 0 else 0
		source_root = local_root if source else remote_root
		target_executors = local_executors if source else remote_executors
		count = c_longlong(0)
		numPartitions = c_longlong(0)
		if source:
			input = self._executor_data.getPartitions()
			count.value = sum(map(len, input))
			numPartitions.value = len(input)
		group.Allreduce(MPI.IN_PLACE, numPartitions, 1, MPI.LONG_LONG, MPI.SUM)
		logger.info("General: importData " + str(numPartitions) + "partitions")
		block = int(numPartitions.value / target_executors)
		remainder = numPartitions.value % target_executors
		last = 0
		for i in range(target_executors):
			end = last + block + 1
			if i < remainder:
				end += 1
			ranges.append((last, end))

		if source_root == 0:
			ranges = [(0, 0) for _ in range(executors - target_executors)] + ranges
		else:
			ranges = [(ranges[-1][1], ranges[-1][1]) for _ in range(executors - target_executors)] + ranges

		global_queue = list()
		m = executors if executors % 2 else executors + 1
		id = 0
		id2 = m * m - 2
		for i in range(m - 1):
			if rank == id % (m - 1):
				global_queue.append(m - 1)
			if rank == m - 1:
				global_queue.append(id % (m - 1))
			id += 1
			for j in range(int(m / 2)):
				if rank == id % (m - 1):
					global_queue.append(id2 % (m - 1))
				if rank == id2 % (m - 1):
					global_queue.append(id % (m - 1))
				id += 1
				id2 -= 1
		for other in global_queue:
			if local_root > 0:
				if other < local_root:
					queue.append(other)
			else:
				if other >= remote_root:
					queue.append(other)
		if source:
			executors_count = (c_longlong * executors)()
			offset = 0
			self._executor_data.mpi().native().Allgather(count, 1, MPI.LONG_LONG, executors_count, 1, MPI.LONG_LONG)
			for i in range(local_rank):
				local_rank += executors_count[i].value
			return offset
		return ranges[rank][0]

	def __joinToGroupImpl(self, id, leader):
		root = self._executor_data.hasVariable("server")
		comm = self._executor_data.mpi().native()
		if leader:
			port = id if root else None
			intercomm = comm.Accept(port)
		else:
			port = id
			intercomm = comm.Connect(port)
		comm1 = intercomm.Merge(not leader)
		intercomm.Free()
		if comm != MPI.COMM_WORLD:
			comm.Free()
		return comm1

	def __getGroup(self, id):
		if id not in self.__groups:
			raise KeyError("Group " + id + " not found")
		return self.__groups[id]
