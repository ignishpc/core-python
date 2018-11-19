import unittest
from .IObjectTest import IObjectTest
from ignis.executor.core.storage.IMemoryObject import IMemoryObject


class IMemoryObjectTest(IObjectTest, unittest.TestCase):

	def getObject(self, elems, _bytes):
		return IMemoryObject(manager=self._manager, native=False, elems=elems, sz=_bytes)


class IMemoryObjectTestNative(IObjectTest, unittest.TestCase):

	def getObject(self, elems, _bytes):
		return IMemoryObject(manager=self._manager, native=True, elems=elems, sz=_bytes)
