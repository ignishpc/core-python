from .IObject import IObject
from ignis.data.ISharedMemoryBuffer import ISharedMemoryBuffer
from .iterator.ICoreIterator import readToWrite
from .iterator.ISimpleIterator import ISimpleReadIterator, ISimpleWriteIterator
from ignis.data.IZlibTransport import IZlibTransport
from ignis.data.IObjectProtocol import IObjectProtocol


class IMemoryObject(IObject):
	class __Index:

		def __init__(self, elems):
			self.__bytes = 5
			self.__shared = ISharedMemoryBuffer(elems * self.__bytes)

		def clear(self):
			if self.__shared.availableWrite() != self.__shared.getBufferSize():
				self.__shared.resetBuffer()

		def __getitem__(self, i):
			int.from_bytes(self.__shared[i * self.__bytes:(i + 1) * self.__bytes], byteorder='little')

		def append(self, value):
			self.__shared.write(value.to_bytes(self.__bytes, byteorder='little'))

	def __init__(self, manager, native=False, elems=1000, sz=50 * 1024 * 1024):
		self.__readOnly = False
		self.__rawMemory = ISharedMemoryBuffer(sz)
		self.__index = IMemoryObject.__Index(elems)
		self._manager = manager
		self._native = native
		self._reader = None
		self._writer = None
		self._protocol = None
		self._elems = 0

	def readIterator(self):
		def hasNext(it):
			return it._elems < self._elems

		def next(it):
			it._elems += 1
			return self._reader.read(self._protocol)

		def skip(it, n):
			it._elems += n
			it.setReadBuffer(self.__index[it._elems], self.__rawMemory.writeEnd())

		if not self.__readOnly:
			self.__rawMemory.flush()
			return self.__readObservation().readIterator()
		return ISimpleReadIterator(next=next, hasNext=hasNext, skip=skip)

	def writeIterator(self):
		def write(it, obj):
			self._elems += 1
			if self._writer is None or self._reader is None:
				if self._native:
					self.__type = None
					self._writer = self._manager.nativeWriter
					self._reader = self._manager.nativeReader
					self._protocol = self.__rawMemory
				else:
					self.__type = type(obj)
					self._writer = self._manager.writer.getWriter(obj)
					self._reader = self._manager.reader.getReader(self._writer.getId())
					self._protocol = IObjectProtocol(self.__rawMemory)
			elif self.__type and self.__type != type(obj):
				raise ValueError("Current serialization does not support heterogeneous types")
			self.__index.append(self.__rawMemory.readEnd())
			self._writer.write(obj, self._protocol)

		return ISimpleWriteIterator(write)

	def read(self, trans):
		self.clear()
		dataTransport = IZlibTransport(trans)
		self.__readHeader(dataTransport)
		if self._native:
			dataProto = dataTransport
		else:
			dataProto = IObjectProtocol(dataTransport)
		for i in range(0, self._elems):
			obj = self._reader.read(dataProto)
			self._writer.write(obj, self._protocol)
			self.__index.append(self.__rawMemory.readEnd())

	def write(self, trans, compression):
		if not self.__readOnly:
			self.__readObservation().write(trans, compression)
			return
		dataTransport = IZlibTransport(trans)
		self.__writeHeader(dataTransport)
		while True:
			buffer = self.__rawMemory.read(256)
			if not buffer:
				break
			dataTransport.write(buffer)
		dataTransport.flush()

	def copyFrom(self, source):
		readToWrite(source.readIterator(), self.writeIterator())

	def moveFrom(self, source):
		self.copyFrom(source)
		source.clear()

	def __len__(self):
		return self._elems

	def clear(self):
		if self.__rawMemory.availableWrite() != self.__rawMemory.getBufferSize():
			self.__rawMemory.resetBuffer()
		self._elems = 0
		self.__index.clear()
		self._writer = None
		self._reader = None

	def __readHeader(self, transport):
		headProto = IObjectProtocol(transport)
		self._native = headProto.readBool()
		if self._native:
			assert headProto.readBool()
		else:
			self._manager.reader.readTypeAux(headProto)
		self._elems = self._manager.reader.readSizeAux(headProto)
		if self._native:
			self._reader = self._manager.nativeReader
			self._writer = self._manager.nativeWriter
			self._protocol = self.__rawMemory
		else:
			self._reader = self._manager.reader.getReader(self._manager.reader.readTypeAux(headProto))
			self._writer = self._manager.writer.getWriterByType(self._reader.getId())
			self._protocol = IObjectProtocol(self.__rawMemory)

	def __writeHeader(self, transport):
		headProto = IObjectProtocol(transport)
		headProto.writeBool(self._native)
		if self._native:
			headProto.writeBool(True)
		else:
			self._manager.writer.getWriter(list()).writeType(headProto)
		self._manager.writer.writeSizeAux(self._elems, headProto)
		if not self._native:
			self._writer.writeType(headProto)

	def fit(self):
		self.__rawMemory.setBufferSize(self.__rawMemory.getBufferSize())

	def __readObservation(self):
		import copy
		buffer = self.__rawMemory.getBuffer()
		obs = ISharedMemoryBuffer(buf=buffer, sz=self.__rawMemory.availableRead())
		object = copy.copy(self)
		object._protocol = obs
		if not self._native:
			object._protocol = self._protocol.__class__(object._protocol)
		object.__rawMemory = obs
		object.__readOnly = True
		return object

