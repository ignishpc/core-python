import logging
import threading

from ignis import IDriverException
from ignis.executor.core.modules.IModule import IModule
from ignis.executor.core.storage import IMemoryPartition
from ignis.rpc.executor.cachecontext.ICacheContextModule import Iface as ICacheContextModuleIface

logger = logging.getLogger(__name__)


class IDriverContext(IModule, ICacheContextModuleIface):

    def __init__(self, executor_data):
        IModule.__init__(self, executor_data, logger)
        self.__lock = threading.Lock()
        self.__next_context_id = 0
        self.__context = dict()

    def saveContext(self):
        try:
            with self.__lock:
                id = self.__next_context_id
                self.__next_context_id += 1
                self.__context[id] = self._executor_data.getPartitions()
                return id
        except Exception as ex:
            self._pack_exception(ex)

    def clearContext(self):
        try:
            with self.__lock:
                self._executor_data.deletePartitions()
                self._executor_data.clearVariables()
        except Exception as ex:
            self._pack_exception(ex)

    def loadContext(self, id):
        try:
            with self.__lock:
                value = self.__context.get(id, None)
                if not value:
                    raise ValueError("context " + str(id) + " not found")
                self._executor_data.setPartitions(value)
                del self.__context.get[id]
        except Exception as ex:
            self._pack_exception(ex)

    def cache(self, id, level):
        pass

    def loadCache(self, id):
        pass

    def parallelize(self, data, collection, native):
        try:
            group = self._executor_data.getPartitionTools().newPartitionGroup()

            if isinstance(collection, bytes):
                partition = IMemoryPartition(native, cls=bytearray, elements=bytearray(collection))
            elif isinstance(collection, bytearray):
                partition = IMemoryPartition(native, cls=bytearray, elements=collection)
            elif isinstance(collection, list):
                partition = IMemoryPartition(native, cls=list, elements=collection)
            elif collection.__class__.__name__ == 'ndarray':
                from ignis.executor.core.io.INumpy import INumpyWrapper as Wrapper
                class INumpyWrapper(Wrapper):
                    def __init__(self):
                        Wrapper.__init__(self, len(collection), collection.dtype)

                return IMemoryPartition(native=native, cls=INumpyWrapper, elements=Wrapper(array=collection))
            else:
                partition = IMemoryPartition(native)
                it = partition.writeIterator()
                for item in collection:
                    it.write(item)

            group.add(partition)

            with self.__lock:
                self._executor_data.setPartitions(group)
                return self.saveContext()
        except Exception as ex:
            raise IDriverException(str(ex)) from ex

    def collect(self, id):
        try:
            with self.__lock:
                self.loadContext(id)
                group = self._executor_data.getPartitions()
                self._executor_data.deletePartitions()

            if self._executor_data.getPartitionTools().isMemory(group):
                cls = group[0]._IMemoryPartition__cls
                result = IMemoryPartition(False, cls=cls)
                for part in group:
                    part.copyTo(result)
                elems = result._IMemoryPartition__elements
                if cls.__name__ == 'INumpyWrapper':
                    return elems.array
                return elems

            result = list()
            for part in group:
                it = part.readIterator()
                while it.hasNext():
                    result.append(it.next())
            return result
        except Exception as ex:
            raise IDriverException(str(ex)) from ex

    def collect1(self, id):
        return self.collect(id)[0];
