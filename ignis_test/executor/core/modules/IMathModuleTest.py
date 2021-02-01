import unittest

from ignis.executor.core.modules.IMathModule import IMathModule
from ignis_test.executor.core.IElements import IElementsInt, IElementsPair
from ignis_test.executor.core.modules.IModuleTest import IModuleTest


class IMathModuleTest(IModuleTest, unittest.TestCase):

    def __init__(self, *args, **kwargs):
        IModuleTest.__init__(self)
        unittest.TestCase.__init__(self, *args, **kwargs)
        self.__math = IMathModule(self._executor_data)
        props = self._executor_data.getContext().props()
        props["ignis.modules.sort.samples"] = "2"

    def test_count(self):
        self._executor_data.getContext().props()["ignis.partition.type"] = "Memory"
        elems = IElementsInt().create(100 * 2, 0)
        self.loadToPartitions(elems, 2)

        self.assertEqual(self.__math.count(), len(elems))

    def test_max(self):
        self._executor_data.getContext().props()["ignis.partition.type"] = "Memory"
        elems = IElementsInt().create(100 * 2, 0)
        local_elems = self.rankVector(elems)
        self.loadToPartitions(local_elems, 2)
        self.__math.max()
        result = self.getFromPartitions()

        if self._executor_data.mpi().isRoot(0):
            elems.sort()
            self.assertEqual(1, len(result))
            self.assertEqual(elems[-1], result[0])

    def test_min(self):
        self._executor_data.getContext().props()["ignis.partition.type"] = "Memory"
        elems = IElementsInt().create(100 * 2, 0)
        local_elems = self.rankVector(elems)
        self.loadToPartitions(local_elems, 2)
        self.__math.min()
        result = self.getFromPartitions()

        if self._executor_data.mpi().isRoot(0):
            elems.sort()
            self.assertEqual(1, len(result))
            self.assertEqual(elems[0], result[0])

    def test_countByKey(self):
        self._executor_data.getContext().props()["ignis.partition.type"] = "Memory"
        np = self._executor_data.getContext().executors()
        elems = IElementsPair((IElementsInt, IElementsInt)).create(100 * 2 * np, 0)
        local_elems = self.rankVector(elems)
        self.loadToPartitions(local_elems, 2)
        self.__math.countByKey()
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
            for key, count in result:
                self.assertEqual(counts[key], count)

    def test_countByValue(self):
        self._executor_data.getContext().props()["ignis.partition.type"] = "Memory"
        np = self._executor_data.getContext().executors()
        elems = IElementsPair((IElementsInt, IElementsInt)).create(100 * 2 * np, 0)
        local_elems = self.rankVector(elems)
        self.loadToPartitions(local_elems, 2)
        self.__math.countByValue()
        result = self.getFromPartitions()

        counts = dict()
        for key, value in elems:
            if value in counts:
                counts[value] += 1
            else:
                counts[value] = 1

        self.loadToPartitions(result, 1)
        self._executor_data.mpi().gather(self._executor_data.getPartitions()[0], 0)
        result = self.getFromPartitions()

        if self._executor_data.mpi().isRoot(0):
            for value, count in result:
                self.assertEqual(counts[value], count)
