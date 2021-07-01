import logging
import math

from ignis.executor.core.modules.impl.IBaseImpl import IBaseImpl

logger = logging.getLogger(__name__)


class IReduceImpl(IBaseImpl):

    def __init__(self, executor_data):
        IBaseImpl.__init__(self, executor_data, logger)

    def __basicReduce(self, f, result):
        input = self._executor_data.getAndDeletePartitions()
        logger.info("Reduce: reducing " + str(len(input)) + " partitions locally")

        acum = None
        for i in range(len(input)):
            part = input[i]
            if len(part) == 0:
                continue
            if acum:
                acum = self.__aggregatePartition(f, part, acum)
            else:
                acum = self.__reducePartition(f, part)
            input[i] = None
        result.writeIterator().write(acum)

    def reduce(self, f):
        context = self._executor_data.getContext()
        f.before(context)
        elem_part = self._executor_data.getPartitionTools().newMemoryPartition(1)
        self.__basicReduce(f, elem_part)
        self.__finalReduce(f, elem_part)
        f.after(context)

    def treeReduce(self, f):
        context = self._executor_data.getContext()
        f.before(context)
        elem_part = self._executor_data.getPartitionTools().newMemoryPartition(1)
        self.__basicReduce(f, elem_part)
        self.__finalTreeReduce(f, elem_part)
        f.after(context)

    def zero(self, f):
        context = self._executor_data.getContext()
        f.before(context)
        self._executor_data.setVariable("zero", f.call(context))
        f.after(context)

    def aggregate(self, f):
        context = self._executor_data.getContext()
        f.before(context)
        output = self._executor_data.getPartitionTools().newPartitionGroup()
        input = self._executor_data.getAndDeletePartitions()
        partial_reduce = self._executor_data.getPartitionTools().newMemoryPartition(1)
        logger.info("Reduce: aggregating " + str(len(input)) + " partitions locally")

        acum = self._executor_data.getVariable("zero")
        for i in range(len(input)):
            part = input[i]
            if len(part) == 0:
                continue
            acum = self.__aggregatePartition(f, part, acum)
            input[i] = None

        partial_reduce.writeIterator().write(acum)
        output.add(partial_reduce)
        self._executor_data.setPartitions(output)

    def fold(self, f):
        context = self._executor_data.getContext()
        f.before(context)
        input = self._executor_data.getAndDeletePartitions()
        partial_reduce = self._executor_data.getPartitionTools().newMemoryPartition(1)
        logger.info("Reduce: folding " + str(len(input)) + " partitions locally")

        acum = self._executor_data.getVariable("zero")
        for i in range(len(input)):
            part = input[i]
            if len(part) == 0:
                continue
            acum = self.__aggregatePartition(f, part, acum)
            input[i] = None

        partial_reduce.writeIterator().write(acum)
        self.__finalReduce(f, partial_reduce)

    def treeFold(self, f):
        context = self._executor_data.getContext()
        f.before(context)
        input = self._executor_data.getAndDeletePartitions()
        partial_reduce = self._executor_data.getPartitionTools().newMemoryPartition(1)
        logger.info("Reduce: folding " + str(len(input)) + " partitions locally")

        acum = self._executor_data.getVariable("zero")
        for i in range(len(input)):
            part = input[i]
            if len(part) == 0:
                continue
            acum = self.__aggregatePartition(f, part, acum)
            input[i] = None

        partial_reduce.writeIterator().write(acum)
        self.__finalTreeReduce(f, partial_reduce)

    def union(self, other):
        input1 = self._executor_data.getAndDeletePartitions()
        input2 = self._executor_data.getVariable(other)
        self._executor_data.removeVariable(other)

        output = self._executor_data.getPartitionTools().newPartitionGroup(len(input1))
        #backend makes all partitions are the same size
        for p in range(len(input1)):
            writer = output[p].writeIterator()
            p1 = input1[p]
            p2 = input2[p]

            for i in range(min(len(p1), len(p2))):
                writer.write((p1[i], p2[i]))

        self._executor_data.setPartitions(output)

    def join(self, other, numPartitions):
        logger.info("Reduce: preparing first partitions")
        self.__keyHashing(numPartitions)
        self.__exchanging()
        input1 = self._executor_data.getAndDeletePartitions()

        logger.info("Reduce: preparing second partitions")
        self._executor_data.setPartitions(self._executor_data.getVariable(other))
        self._executor_data.removeVariable(other)
        self.__keyHashing(numPartitions)
        self.__exchanging()
        input2 = self._executor_data.getAndDeletePartitions()

        output = self._executor_data.getPartitionTools().newPartitionGroup(numPartitions)
        logger.info("Reduce: joining key elements")

        acum = dict()
        for p in range(len(input1)):
            for key, value in input1[p]:
                if key in acum:
                    acum[key].append(value)
                else:
                    acum[key] = [value]
            input1[p] = None
            writer = output[p].writeIterator()

            for key, value2 in input2[p]:
                if key in acum:
                    for _, value1 in acum[key]:
                        writer.write((key, (value1, value2)))
            input2[p] = None
            acum.clear()

        self._executor_data.setPartitions(output)

    def distinct(self, numPartitions):
        self.hashing(numPartitions)
        self.__exchanging()

        input = self._executor_data.getAndDeletePartitions()
        output = self._executor_data.getPartitionTools().newPartitionGroup(numPartitions)
        logger.info("Reduce: removing duplicate elements")

        single = set()
        for p in range(len(input)):
            for item in input[p]:
                single.add(item)
            input[p] = None
            writer = output[p].writeIterator()
            for item in single:
                writer.write(item)
            single.clear()

        self._executor_data.setPartitions(output)

    def groupByKey(self, numPartitions):
        self.__keyHashing(numPartitions)
        self.__exchanging()

        input = self._executor_data.getAndDeletePartitions()
        output = self._executor_data.getPartitionTools().newPartitionGroup(numPartitions)
        logger.info("Reduce: reducing key elements")

        acum = dict()
        for p in range(len(input)):
            for key, value in input[p]:
                if key in acum:
                    acum[key].append(value)
                else:
                    acum[key] = [value]
            input[p] = None
            writer = output[p].writeIterator()
            for item in acum.items():
                writer.write(item)
            acum.clear()

        self._executor_data.setPartitions(output)

    def reduceByKey(self, f, numPartitions, localReduce):
        context = self._executor_data.getContext()
        f.before(context)
        if localReduce:
            logger.info("Reduce: local reducing key elements")
            self.__localReduceByKey(f)
        self.__keyHashing(numPartitions)
        self.__exchanging()
        logger.info("Reduce: reducing key elements")

        self.__localReduceByKey(f)
        f.after(context)

    def aggregateByKey(self, f, numPartitions, hashing):
        context = self._executor_data.getContext()
        f.before(context)
        if hashing:
            self.__keyHashing(numPartitions)
            self.__exchanging()
        logger.info("Reduce: aggregating key elements")

        self.__localAggregateByKey(f)
        f.after(context)

    def foldByKey(self, f, numPartitions, localFold):
        context = self._executor_data.getContext()
        f.before(context)

        if localFold:
            logger.info("Reduce: local folding key elements")
            self.__localAggregateByKey(f)
            self.__keyHashing(numPartitions)
            self.__exchanging()
            logger.info("Reduce: folding key elements")
            self.__localReduceByKey(f)
        else:
            self.__keyHashing(numPartitions)
            self.__exchanging()
            logger.info("Reduce: folding key elements")
            self.__localAggregateByKey(f)

    def __reducePartition(self, f, part):
        context = self._executor_data.getContext()
        reader = part.readIterator()
        acum = reader.next()
        for item in reader:
            acum = f.call(acum, item, context)
        return acum

    def __finalReduce(self, f, partial):
        output = self._executor_data.getPartitionTools().newPartitionGroup()
        # logger.info("Reduce: reducing all elements in the executor")
        # Python is single core, len(partial) is always 1
        logger.info("Reduce: gathering elements for an executor")
        self._executor_data.mpi().gather(partial, 0)
        if self._executor_data.mpi().isRoot(0) and len(partial) > 0:
            logger.info("Reduce: final reduce")
            result = self._executor_data.getPartitionTools().newMemoryPartition(1)
            result.writeIterator().write(self.__reducePartition(f, partial))
            output.add(result)
        self._executor_data.setPartitions(output)

    def __finalTreeReduce(self, f, partial):
        executors = self._executor_data.mpi().executors()
        rank = self._executor_data.mpi().rank()
        context = self._executor_data.getContext()
        pivotUp = executors
        output = self._executor_data.getPartitionTools().newPartitionGroup()
        # logger.info("Reduce: reducing all elements in the executor")
        # Python is single core, len(partial) is always 1

        logger.info("Reduce: performing a final tree reduce")
        while pivotUp > 1:
            pivotDown = math.floor(pivotUp / 2)
            pivotUp = math.ceil(pivotUp / 2)
            if rank < pivotDown:
                self._executor_data.mpi().recv(partial, rank + pivotUp, 0)
                result = f.call(partial[0], partial[1], context)
                partial.clear()
                partial.writeIterator().write(result)
            elif rank >= pivotUp:
                self._executor_data.mpi().send(partial, rank - pivotUp, 0)

        if self._executor_data.mpi().isRoot(0) and len(partial) > 0:
            result = self._executor_data.getPartitionTools().newMemoryPartition(1)
            result.writeIterator().write(partial[0])
            output.add(result)
        self._executor_data.setPartitions(output)

    def __aggregatePartition(self, f, part, acum):
        context = self._executor_data.getContext()
        for item in part:
            acum = f.call(acum, item, context)
        return acum

    def __localReduceByKey(self, f):
        input = self._executor_data.getPartitions()
        output = self._executor_data.getPartitionTools().newPartitionGroup()
        context = self._executor_data.getContext()
        acum = dict()
        for p in range(len(input)):
            for key, value in input[p]:
                if key in acum:
                    acum[key] = f.call(acum[key], value, context)
                else:
                    acum[key] = value

        output.add(self._executor_data.getPartitionTools().newMemoryPartition())
        writer = output[0].writeIterator()
        for item in acum.items():
            writer.write(item)

        self._executor_data.setPartitions(output)

    def __localAggregateByKey(self, f):
        input = self._executor_data.getAndDeletePartitions()
        output = self._executor_data.getPartitionTools().newPartitionGroup()
        context = self._executor_data.getContext()
        base_acum = self._executor_data.getVariable("zero")

        acum = dict()
        for p in range(len(input)):
            for key, value in input[p]:
                if key in acum:
                    acum[key] = f.call(acum[key], value, context)
                else:
                    acum[key] = f.call(base_acum, value, context)
            input[p] = None

        output.add(self._executor_data.getPartitionTools().newMemoryPartition())
        writer = output[0].writeIterator()
        for item in acum.items():
            writer.write(item)

        self._executor_data.setPartitions(output)

    def hashing(self, numPartitions):
        input = self._executor_data.getAndDeletePartitions()
        output = self._executor_data.getPartitionTools().newPartitionGroup(numPartitions)
        cache = input.cache()
        logger.info("Reduce: creating " + str(len(input)) + " new partitions with key hashing")

        writers = [part.writeIterator() for part in output]
        n = len(writers)
        for i in range(len(input)):
            part = input[i]
            for elem in part:
                writers[hash(elem) % n].write(elem)
            if not cache:
                part.clear()
            input[i] = None

        self._executor_data.setPartitions(output)

    def __keyHashing(self, numPartitions):
        input = self._executor_data.getAndDeletePartitions()
        output = self._executor_data.getPartitionTools().newPartitionGroup(numPartitions)
        cache = input.cache()
        logger.info("Reduce: creating " + str(len(input)) + " new partitions with key hashing")

        writers = [part.writeIterator() for part in output]
        n = len(writers)
        for i in range(len(input)):
            part = input[i]
            for elem in part:
                writers[hash(elem[0]) % n].write(elem)
            if not cache:
                part.clear()
            input[i] = None

        self._executor_data.setPartitions(output)

    def __exchanging(self):
        input = self._executor_data.getPartitions()
        output = self._executor_data.getPartitionTools().newPartitionGroup()
        numPartitions = len(input)
        logger.info("Reduce: exchanging " + str(numPartitions) + " partitions")

        self.exchange(input, output)

        self._executor_data.setPartitions(output)
