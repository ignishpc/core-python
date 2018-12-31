from .IObject import IObject
from .iterator.IMemoryIterator import readToWrite, IMemoryReadIterator, IMemoryWriteIterator
from ignis.data.IObjectProtocol import IObjectProtocol
from ignis.data.IZlibTransport import IZlibTransport

class IMemoryObject(IObject):
	TYPE = "memory"

	def __init__(self, manager, native):
		self.__manager = manager
		self.__native = native
		self._elems = 0
		self._data = list()

	def readIterator(self):
		return IMemoryReadIterator(self)

	def writeIterator(self):
		return IMemoryWriteIterator(self)

	def read(self, trans):
		self.clear()
		ctrans = IZlibTransport(trans)
		proto = IObjectProtocol(ctrans)
		self._data = proto.readObject(self.__manager)
		self._elems = len(self._data)

	def write(self, trans, compression):
		ctrans = IZlibTransport(trans, compression)
		proto = IObjectProtocol(ctrans)
		proto.writeObject(self._data, self.__manager, self.__native, self.__native)
		ctrans.flush()

	def copyFrom(self, source):
		readToWrite(source.readIterator(), self.writeIterator())

	def moveFrom(self, source):
		if self._elems == 0 and type(self) == type(source):
			self._data, source._data = source._data, self._data
			self._elems, source._elems = source._elems, self._elems
		else:
			self.copyFrom(source)
		source.clear()

	def clear(self):
		self._elems = 0
		self._data.clear()

	def __len__(self):
		return self._elems

	def __getstate__(self):
		raise ValueError("This object can never be serialized")
