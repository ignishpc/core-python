from ignis.executor.core.storage import IMemoryPartition


class IBaseImpl:
    def __init__(self, executor_data, logger):
        self._executor_data = executor_data
        self.__logger = logger

    def resizeMemoryPartition(self, part, n):
        inner = part._inner()
        cls = part._IMemoryPartition__cls
        if cls == bytearray or cls == list:
            part._IMemoryPartition__elements = inner[0:n]
        elif cls.__name__ == 'INumpyWrapper':
            part.array = part.array[0:n]
            part._INumpy__next = n
        else:
            newPart = IMemoryPartition(part._native, cls)
            writer = newPart.writeIterator()
            it = part.readIterator()
            for i in range(n):
                writer.write(it.next())

    def exchange(self, input, output):
        self._executor_data.mpi().exchange(input, output)
