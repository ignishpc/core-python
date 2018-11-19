import unittest
from .IObjectTest import IObjectTest
from ignis.executor.core.storage.IRawMemoryObject import IRawMemoryObject


class IRawMemoryObjectTest(IObjectTest, unittest.TestCase):

	def getObject(self, elems, _bytes):
		return IRawMemoryObject(compression=6, manager=self._manager, native=False, sz=_bytes)


class IRawMemoryObjectTestNative(IObjectTest, unittest.TestCase):

	def getObject(self, elems, _bytes):
		return IRawMemoryObject(compression=6, manager=self._manager, native=True, sz=_bytes)
