from .IRawObject import IRawObject, IObjectProtocol
from ignis.data.IMemoryBuffer import IMemoryBuffer
from ignis.data.IZlibTransport import IZlibTransport


class IRawMemoryObject(IRawObject):
	TYPE = "raw memory"

	def __init__(self, compression, manager, native=False, sz=50 * 1024 * 1024, shared=False):
		self._readOnly = False
		self._rawMemory = IMemoryBuffer(sz, shared=shared)
		self._compression = compression
		super().__init__(IZlibTransport(self._rawMemory, compression), manager, native)

	def readIterator(self):
		if not self._readOnly:
			return self.__readObservation().readIterator()
		return super().readIterator()

	def writeIterator(self):
		return super().writeIterator()

	def read(self, trans):
		super().read(trans)

	def write(self, trans, compression):
		if not self._readOnly:
			self.__readObservation().write(trans, compression)
			return
		super().write(trans, compression)

	def copyTo(self, target):
		if not self._readOnly:
			self.__readObservation().copyTo(target)
			return
		super().copyTo(target)

	def clear(self):
		super().clear()
		self._transport.restart()
		self._rawMemory.setWriteBuffer(0)
		self._rawMemory.setReadBuffer(0)

	def fit(self):
		self._flush()
		self._rawMemory.setBufferSize(self._rawMemory.getBufferSize())

	def __setstate__(self, st):
		self.__dict__.update(st)
		self._transport = IZlibTransport(self._rawMemory, self._compression)
		if self._native:
			self._protocol = self._transport
		else:
			self._protocol = IObjectProtocol(self._transport)

	def __getstate__(self):
		st = self.__dict__.copy()
		del st["_protocol"]
		del st["_transport"]
		return st

	def __readObservation(self):
		self._flush()
		obs = IRawMemoryObject.__new__(IRawMemoryObject)
		st = self.__getstate__()
		st["_readOnly"] = True
		st["_rawMemory"] = IMemoryBuffer(buf=self._rawMemory.getBuffer(),sz=self._rawMemory.availableRead())
		obs.__setstate__(st)
		return obs

	def _flush(self):
		if not self._readOnly:
			super()._flush()
