from .IRawObject import IRawObject
from ignis.data.ISharedMemoryBuffer import ISharedMemoryBuffer
from ignis.data.IZlibTransport import IZlibTransport


class IRawMemoryObject(IRawObject):
	TYPE = "raw memory"

	def __init__(self, compression, manager, native=False, sz=50 * 1024 * 1024):
		self.__readOnly = False
		self.__rawMemory = ISharedMemoryBuffer(sz)
		super().__init__(IZlibTransport(self.__rawMemory, compression), compression, manager, native)

	def readIterator(self):
		if not self.__readOnly:
			self._transport.flush()
			return self.__readObservation().readIterator()
		return super().readIterator()

	def writeIterator(self):
		return super().writeIterator()

	def read(self, trans):
		super().read(trans)

	def write(self, trans, compression):
		if not self.__readOnly:
			self._transport.flush()
			self.__readObservation().write(trans, compression)
			return
		super().write(trans, compression)

	def clear(self):
		super().clear()
		self._transport.restart()
		if self.__rawMemory.availableWrite() != self.__rawMemory.getBufferSize():
			self.__rawMemory.resetBuffer()

	def fit(self):
		self._transport.flush()
		self.__rawMemory.setBufferSize(self.__rawMemory.getBufferSize())

	def __readObservation(self):
		import copy
		self._transport.flush()
		buffer = self.__rawMemory.getBuffer()
		obs = ISharedMemoryBuffer(buf=buffer, sz=self.__rawMemory.availableRead())
		object = copy.copy(self)
		object._protocol = IZlibTransport(obs)
		if not self._native:
			object._protocol = self._protocol.__class__(object._protocol)
		object.__rawMemory = obs
		object.__readOnly = True
		return object
