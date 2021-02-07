import unittest

from ignis.executor.core.io import INumpy
from ignis.executor.core.modules.IGeneralModule import IGeneralModule
from ignis_test.executor.core.IElements import IElementsInt, IElementsStr, IElementsBytes, IElementsPair
from ignis_test.executor.core.modules.IModuleTest import IModuleTest


class IGeneralModuleTest(IModuleTest, unittest.TestCase):

    def __init__(self, *args, **kwargs):
        IModuleTest.__init__(self)
        unittest.TestCase.__init__(self, *args, **kwargs)
        self.__general = IGeneralModule(self._executor_data)
        props = self._executor_data.getContext().props()
        props["ignis.modules.sort.samples"] = "0.1"

    def test_mapInt(self):
        self.__mapTest("MapInt", "Memory", IElementsInt)

    def test_filterInt(self):
        self.__filterTest("FilterInt", "RawMemory", IElementsInt)

    def test_flatmapString(self):
        self.__flatmapTest("FlatmapString", "Memory", IElementsStr)

    def test_keyByStringInt(self):
        self.__keyByTest("KeyByString", "RawMemory", IElementsStr)

    def test_mapPartitionsInt(self):
        self.__mapPartitionsTest("MapPartitionsInt", "Memory", IElementsInt)

    def test_mapPartitionWithIndexInt(self):
        self.__mapPartitionsWithIndexTest("MapPartitionWithIndexInt", "RawMemory", IElementsInt)

    def test_mapExecutorInt(self):
        self.__mapExecutorTest("MapExecutorInt", "RawMemory", IElementsInt)

    def test_mapExecutorToString(self):
        self.__mapExecutorToTest("MapExecutorToString", "Memory", IElementsInt)

    def test_groupByIntString(self):
        self.__groupByTest("GroupByIntString", "Memory", IElementsStr)

    def test_sortBasicInt(self):
        self.__sortTest(None, "Memory", IElementsInt)

    def test_sortInt(self):
        self.__sortTest("SortInt", "Memory", IElementsInt)

    def test_sortIntBytes(self):
        self._executor_data.getContext().vars()['STORAGE_CLASS'] = bytearray
        self.__sortTest("SortInt", "Memory", IElementsBytes)

    def test_sortIntNumpy(self):
        INumpy.enable()
        import numpy
        self._executor_data.getContext().vars()['STORAGE_CLASS'] = numpy.ndarray
        self._executor_data.getContext().vars()['STORAGE_CLASS_DTYPE'] = numpy.int64
        self.__sortTest("SortInt", "Memory", IElementsBytes)
        INumpy.disable()

    def test_sortString(self):
        self.__sortTest("SortString", "RawMemory", IElementsStr)

    def test_flatMapValuesInt(self):
        self.__flatMapValuesTest("FlatMapValuesInt", "Memory", (IElementsStr, IElementsInt))

    def test_mapValuesInt(self):
        self.__mapValuesTest("MapValuesInt", "RawMemory", (IElementsStr, IElementsInt))

    def test_groupByKeyIntString(self):
        self.__groupByKeyTest("Memory", (IElementsInt, IElementsStr))

    def test_reduceByKeyIntString(self):
        self.__reduceByKeyTest("ReduceString", "RawMemory", (IElementsInt, IElementsStr))

    def test_aggregateByKeyIntInt(self):
        self.__aggregateByKeyTest("ZeroString", "ReduceIntToString", "ReduceString", "Memory",
                                  (IElementsInt, IElementsInt))

    def test_foldByKeyIntInt(self):
        self.__foldByKeyTest("ZeroInt", "ReduceInt", "Memory", (IElementsInt, IElementsInt))

    def test_sortByKeyIntString(self):
        self.__sortByKeyTest("Memory", (IElementsInt, IElementsStr))

    # -------------------------------------Impl-------------------------------------

    def __mapTest(self, name, partitionType, IElements):
        self._executor_data.getContext().props()["ignis.partition.type"] = partitionType
        elems = IElements().create(100 * 2, 0)
        self.loadToPartitions(elems, 2)
        self.__general.map_(self.newSource(name))
        result = self.getFromPartitions()

        self.assertEqual(list(map(str, elems)), result)

    def __filterTest(self, name, partitionType, IElements):
        self._executor_data.getContext().props()["ignis.partition.type"] = partitionType
        elems = IElements().create(100 * 2, 0)
        self.loadToPartitions(elems, 2)
        self.__general.filter(self.newSource(name))
        result = self.getFromPartitions()

        j = 0
        for i in range(len(elems)):
            if elems[i] % 2 == 0:
                self.assertEqual(elems[i], result[j])
                j += 1

    def __flatmapTest(self, name, partitionType, IElements):
        self._executor_data.getContext().props()["ignis.partition.type"] = partitionType
        elems = IElements().create(100 * 2, 0)
        self.loadToPartitions(elems, 2)
        self.__general.flatmap(self.newSource(name))
        result = self.getFromPartitions()

        for i in range(len(elems)):
            self.assertEqual(elems[i], result[2 * i])
            self.assertEqual(elems[i], result[2 * i + 1])

    def __keyByTest(self, name, partitionType, IElements):
        self._executor_data.getContext().props()["ignis.partition.type"] = partitionType
        elems = IElements().create(100 * 2, 0)
        self.loadToPartitions(elems, 2)
        self.__general.keyBy(self.newSource(name))
        result = self.getFromPartitions()

        for i in range(len(elems)):
            self.assertEqual(len(elems[i]), result[i][0])
            self.assertEqual(elems[i], result[i][1])

    def __mapPartitionsTest(self, name, partitionType, IElements):
        self._executor_data.getContext().props()["ignis.partition.type"] = partitionType
        elems = IElements().create(100 * 2, 0)
        self.loadToPartitions(elems, 2)
        self.__general.mapPartitions(self.newSource(name))
        result = self.getFromPartitions()

        self.assertEqual(list(map(str, elems)), result)

    def __mapPartitionsWithIndexTest(self, name, partitionType, IElements):
        self._executor_data.getContext().props()["ignis.partition.type"] = partitionType
        elems = IElements().create(100 * 2, 0)
        self.loadToPartitions(elems, 2)
        self.__general.mapPartitionsWithIndex(self.newSource(name), True)
        result = self.getFromPartitions()

        self.assertEqual(list(map(str, elems)), result)

    def __mapExecutorTest(self, name, partitionType, IElements):
        self._executor_data.getContext().props()["ignis.partition.type"] = partitionType
        elems = IElements().create(100 * 2, 0)
        self.loadToPartitions(elems, 5)
        self.__general.mapExecutor(self.newSource(name))
        result = self.getFromPartitions()

        for i in range(len(elems)):
            self.assertEqual(elems[i] + 1, result[i])

    def __mapExecutorToTest(self, name, partitionType, IElements):
        self._executor_data.getContext().props()["ignis.partition.type"] = partitionType
        elems = IElements().create(100 * 2, 0)
        self.loadToPartitions(elems, 5)
        self.__general.mapExecutorTo(self.newSource(name))
        result = self.getFromPartitions()

        self.assertEqual(list(map(str, elems)), result)

    def __groupByTest(self, name, partitionType, IElements):
        self._executor_data.getContext().props()["ignis.partition.type"] = partitionType
        np = self._executor_data.getContext().executors()
        elems = IElements().create(100 * 2 * np, 0)
        local_elems = self.rankVector(elems)
        self.loadToPartitions(local_elems, 2)
        self.__general.groupBy(self.newSource(name), 2)
        result = self.getFromPartitions()

        counts = dict()
        for elem in elems:
            if len(elem) in counts:
                counts[len(elem)] += 1
            else:
                counts[len(elem)] = 1

        self.loadToPartitions(result, 1)
        self._executor_data.mpi().gather(self._executor_data.getPartitions()[0], 0)
        result = self.getFromPartitions()

        if self._executor_data.mpi().isRoot(0):
            for item in result:
                self.assertEqual(counts[item[0]], len(item[1]))

    def __sortTest(self, name, partitionType, IElements):
        self._executor_data.getContext().props()["ignis.partition.type"] = partitionType
        np = self._executor_data.getContext().executors()
        elems = IElements().create(100 * 4 * np, 0)
        local_elems = self.rankVector(elems)
        self.loadToPartitions(local_elems, 4)
        if name is None:
            self.__general.sort(True)
        else:
            self.__general.sortBy(self.newSource(name), True)
        result = self.getFromPartitions()

        for i in range(1, len(result)):
            self.assertGreaterEqual(result[i], result[i - 1])

        self.loadToPartitions(result, 1)
        self._executor_data.mpi().gather(self._executor_data.getPartitions()[0], 0)

        result = self.getFromPartitions()

        if self._executor_data.mpi().isRoot(0):
            elems.sort()
            for i in range(1, len(result)):
                self.assertEqual(result[i], elems[i])

    def __flatMapValuesTest(self, name, partitionType, IElements):
        self._executor_data.getContext().props()["ignis.partition.type"] = partitionType
        elems = IElementsPair(IElements).create(100 * 2, 0)
        self.loadToPartitions(elems, 2)
        self.__general.flatMapValues(self.newSource(name))
        result = self.getFromPartitions()

        for i in range(len(result)):
            self.assertEqual(elems[i][0], result[i][0])
            self.assertEqual(str(elems[i][1]), result[i][1])

    def __mapValuesTest(self, name, partitionType, IElements):
        self._executor_data.getContext().props()["ignis.partition.type"] = partitionType
        elems = IElementsPair(IElements).create(100 * 2, 0)
        self.loadToPartitions(elems, 2)
        self.__general.mapValues(self.newSource(name))
        result = self.getFromPartitions()

        for i in range(len(result)):
            self.assertEqual(elems[i][0], result[i][0])
            self.assertEqual(str(elems[i][1]), result[i][1])

    def __groupByKeyTest(self, partitionType, IElements):
        self._executor_data.getContext().props()["ignis.partition.type"] = partitionType
        np = self._executor_data.getContext().executors()
        elems = IElementsPair(IElements).create(100 * 2 * np, 0)
        local_elems = self.rankVector(elems)
        self.loadToPartitions(local_elems, 2)
        self.__general.groupByKey(2)
        result = self.getFromPartitions()

        counts = dict()
        for key, value in elems:
            if key in counts:
                counts[key] += 1
            else:
                counts[key] = 1

        self.loadToPartitions(result, 1)
        self._executor_data.mpi().gather(self._executor_data.getPartitions()[0], 0)
        result = self.getFromPartitions()

        if self._executor_data.mpi().isRoot(0):
            for item in result:
                self.assertEqual(counts[item[0]], len(item[1]))

    def __reduceByKeyTest(self, name, partitionType, IElements):
        self._executor_data.getContext().props()["ignis.partition.type"] = partitionType
        np = self._executor_data.getContext().executors()
        elems = IElementsPair(IElements).create(100 * 2 * np, 0)
        local_elems = self.rankVector(elems)
        self.loadToPartitions(local_elems, 2)
        self.__general.reduceByKey(self.newSource(name), 2, True)
        result = self.getFromPartitions()

        counts = dict()
        for key, value in elems:
            if key in counts:
                counts[key] += value
            else:
                counts[key] = value

        self.loadToPartitions(result, 1)
        self._executor_data.mpi().gather(self._executor_data.getPartitions()[0], 0)
        result = self.getFromPartitions()

        if self._executor_data.mpi().isRoot(0):
            for item in result:
                self.assertEqual(counts[item[0]], item[1])

    def __aggregateByKeyTest(self, zero, seq, comb, partitionType, IElements):
        self._executor_data.getContext().props()["ignis.partition.type"] = partitionType
        np = self._executor_data.getContext().executors()
        elems = IElementsPair(IElements).create(100 * 2 * np, 0)
        local_elems = self.rankVector(elems)
        self.loadToPartitions(local_elems, 2)
        self.__general.aggregateByKey4(self.newSource(zero), self.newSource(seq), self.newSource(comb), 2)
        result = self.getFromPartitions()

        counts = dict()
        for key, value in elems:
            if key in counts:
                counts[key] += str(value)
            else:
                counts[key] = str(value)

        self.loadToPartitions(result, 1)
        self._executor_data.mpi().gather(self._executor_data.getPartitions()[0], 0)
        result = self.getFromPartitions()

        if self._executor_data.mpi().isRoot(0):
            for item in result:
                self.assertEqual(counts[item[0]], item[1])

    def __foldByKeyTest(self, zero, name, partitionType, IElements):
        self._executor_data.getContext().props()["ignis.partition.type"] = partitionType
        np = self._executor_data.getContext().executors()
        elems = IElementsPair(IElements).create(100 * 2 * np, 0)
        local_elems = self.rankVector(elems)
        self.loadToPartitions(local_elems, 2)
        self.__general.foldByKey(self.newSource(zero), self.newSource(name), 2, True)
        result = self.getFromPartitions()

        counts = dict()
        for key, value in elems:
            if key in counts:
                counts[key] += value
            else:
                counts[key] = value

        self.loadToPartitions(result, 1)
        self._executor_data.mpi().gather(self._executor_data.getPartitions()[0], 0)
        result = self.getFromPartitions()

        if self._executor_data.mpi().isRoot(0):
            for item in result:
                self.assertEqual(counts[item[0]], item[1])

    def __sortByKeyTest(self, partitionType, IElements):
        self._executor_data.getContext().props()["ignis.partition.type"] = partitionType
        np = self._executor_data.getContext().executors()
        elems = IElementsPair(IElements).create(100 * 4 * np, 0)
        local_elems = self.rankVector(elems)
        self.loadToPartitions(local_elems, 4)
        self.__general.sortByKey(True)
        result = self.getFromPartitions()

        for i in range(1, len(result)):
            self.assertGreaterEqual(result[i], result[i - 1])

        self.loadToPartitions(result, 1)
        self._executor_data.mpi().gather(self._executor_data.getPartitions()[0], 0)

        result = self.getFromPartitions()

        if self._executor_data.mpi().isRoot(0):
            elems.sort()
            for i in range(1, len(result)):
                self.assertEqual(result[i][0], elems[i][0])
