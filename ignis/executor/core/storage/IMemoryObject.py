from .IRawObject import IRawObject
from .iterator.IMemoryIterator import readToWrite, IMemoryReadIterator, IMemoryWriteIterator, IObjectProtocol
from ignis.data.IMemoryBuffer import IMemoryBuffer


class IMemoryObject(IRawObject):
	TYPE = "memory"

	class __Index:

		def __init__(self, elems, shared):
			self.__bytes = 5
			self.__shared = IMemoryBuffer(elems * self.__bytes, shared=shared)

		def clear(self):
			self.__shared.setWriteBuffer(0)
			self.__shared.setReadBuffer(0)

		def __getitem__(self, i):
			return int.from_bytes(self.__shared[i * self.__bytes:(i + 1) * self.__bytes], byteorder='little')

		def append(self, value):
			self.__shared.write(value.to_bytes(self.__bytes, byteorder='little'))

	def __init__(self, manager, native=False, elems=1000, sz=50 * 1024 * 1024, shared=False):
		self._readOnly = False
		self._rawMemory = IMemoryBuffer(sz, shared=shared)
		self._index = IMemoryObject.__Index(elems, shared)
		super().__init__(self._rawMemory, manager, native)

	def readIterator(self):
		if not self._readOnly:
			return self.__readObservation().readIterator()
		return IMemoryReadIterator(self)

	def writeIterator(self):
		return IMemoryWriteIterator(self)

	def copyTo(self, target):
		if not self._readOnly:
			self.__readObservation().copyTo(target)
			return
		super().copyTo(target)

	def copyFrom(self, source):
		readToWrite(source.readIterator(), self.writeIterator())

	def read(self, trans):
		super().read(trans)

	def write(self, trans, compression):
		if not self._readOnly:
			self.__readObservation().write(trans, compression)
			return
		super().write(trans, compression)

	def clear(self):
		super().clear()
		self._index.clear()
		self._rawMemory.setWriteBuffer(0)
		self._rawMemory.setReadBuffer(0)

	def __setstate__(self, st):
		self.__dict__.update(st)
		self._transport = self._rawMemory
		if self._native:
			self._protocol = self._transport
		else:
			self._protocol = IObjectProtocol(self._transport)

	def __getstate__(self):
		self._flush()
		st = self.__dict__.copy()
		del st["_protocol"]
		del st["_transport"]
		return st

	def __readObservation(self):
		self._flush()
		obs = IMemoryObject.__new__(IMemoryObject)
		st = self.__getstate__()
		st["_readOnly"] = True
		st["_rawMemory"] = IMemoryBuffer(buf=self._rawMemory.getBuffer(), sz=self._rawMemory.availableRead())
		obs.__setstate__(st)
		return obs

	def _flush(self):
		if not self._readOnly:
			super()._flush()
