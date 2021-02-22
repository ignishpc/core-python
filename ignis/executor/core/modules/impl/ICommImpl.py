import ctypes
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
        IBaseImpl.__init__(self, executor_data)
        self.__groups = dict()

    def createGroup(self):
        logger.info("Comm: creating group")
        port_name = MPI.Open_port()

        class Handle:
            def __del__(self):  # Close port on context clear
                MPI.Close_port(port_name)

        logger.info("Comm: group created on " + str(port_name))
        self._executor_data.setVariable("server", Handle())
        return port_name

    def joinGroupMembers(self, group_name, id, size):
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

            for i in range(1, size):
                client_comm = MPI.COMM_SELF.Accept(group_name, MPI.INFO_NULL, 0)
                pos = ctypes.c_longlong(0)
                client_comm.Recv((pos, 1, MPI.LONG_LONG), 0, 1963)
                logger.info("Comm: member " + str(id) + " found")
                client_comms[pos.value] = client_comm
            logger.info("Comm: all members found")

            for i in range(1, size):
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
            client_comm.Send((pos, 1, MPI.LONG_LONG), 0, 1963)
            client_comm.Recv((flag, 1, MPI.BOOL), 0, 1963)
            info_comm = MPI.COMM_SELF.Connect(group_name, MPI.INFO_NULL, 0)
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

    def joinToGroup(self, group_name, id):
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

    def __addComm(self, group, comm, local, detroyGroup):
        peer = MPI.COMM_NULL
        up = group == MPI.COMM_NULL

        if comm != MPI.COMM_NULL:
            peer = comm.Merge(up)

        if up:  # new member
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
        return id in self.__groups.keys()

    def destroyGroup(self, id):
        group = self.__groups.get(id, None)
        if group:
            group.Free()
            del self.__groups[id]

    def destroyGroups(self):
        for key, group in self.__groups.items():
            group.Free()
        self.__groups.clear()

    def getProtocol(self):
        return IObjectProtocol.PYTHON_PROTOCOL

    def getPartitions(self, protocol, minPartitions):
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

    def driverGather(self, id):
        raise NotImplementedError()

    def driverGather0(self, id):
        raise NotImplementedError()

    def driverScatter(self, id):
        raise NotImplementedError()

    def send(self, id, partition, dest, tag):
        raise NotImplementedError()

    def recv(self, id, partition, source, tag):
        raise NotImplementedError()
