import unittest
from .IObjectTest import IObjectTest
from ignis.executor.core.storage.IRawIndexMemoryObject import IRawIndexMemoryObject


class IRawIndexMemoryObjectTest(IObjectTest, unittest.TestCase):

	def getObject(self, elems, _bytes):
		return IRawIndexMemoryObject(manager=self._manager, native=False, elems=elems, sz=_bytes)


class IRawIndexMemoryObjectTestNative(IObjectTest, unittest.TestCase):

	def getObject(self, elems, _bytes):
		return IRawIndexMemoryObject(manager=self._manager, native=True, elems=elems, sz=_bytes)
