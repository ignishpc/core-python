from .IObject import IObject
from ignis.data.ISharedMemoryBuffer import ISharedMemoryBuffer, Value
from .iterator.ICoreIterator import readToWrite
from .iterator.ISimpleIterator import ISimpleReadIterator, ISimpleWriteIterator
from .iterator.EmptyIterator import IEmptyReadIterator
from ignis.data.IZlibTransport import IZlibTransport
from ignis.data.IObjectProtocol import IObjectProtocol
from ctypes import c_int64, c_bool


class IMemoryObject(IObject):
	TYPE = "memory"

	class __Index:

		def __init__(self, elems):
			self.__bytes = 5
			self.__shared = ISharedMemoryBuffer(elems * self.__bytes)

		def clear(self):
			if self.__shared.availableWrite() != self.__shared.getBufferSize():
				self.__shared.resetBuffer()

		def __getitem__(self, i):
			return int.from_bytes(self.__shared[i * self.__bytes:(i + 1) * self.__bytes], byteorder='little')

		def append(self, value):
			self.__shared.write(value.to_bytes(self.__bytes, byteorder='little'))

	def __init__(self, manager, native=False, elems=1000, sz=50 * 1024 * 1024):
		self._type_id_ = Value(c_int64, lock=False)
		self._native_ = Value(c_bool, native, lock=False)
		self._elems_ = Value(c_int64, 0, lock=False)
		self.__readOnly = False
		self.__rawMemory = ISharedMemoryBuffer(int(sz / 10))
		self.__index = IMemoryObject.__Index(elems)
		self._manager = manager
		self._reader = None
		self._writer = None
		self._type_id = None
		self._protocol = None

	def readIterator(self):
		if self._elems == 0:
			return IEmptyReadIterator()
		def hasNext(it):
			return it._elems < self._elems

		def next(it):
			it._elems += 1
			return self._reader.read(self._protocol)

		def skip(it, n):
			it._elems += n
			self.__rawMemory.setReadBuffer(self.__index[it._elems])

		if not self.__readOnly:
			self.__rawMemory.flush()
			self.__checkManager()
			return self.__readObservation().readIterator()
		return ISimpleReadIterator(next=next, hasNext=hasNext, skip=skip)

	def writeIterator(self):
		def write(it, obj):
			self._elems += 1
			if self._writer is None:
				if not self._native:
					new_id = self._manager.writer.getWriter(obj).getId()
					if self._type_id and self._type_id != new_id:
						raise ValueError("Current serialization does not support heterogeneous types")
					self._type_id = new_id
					it._type = type(obj)
				self.__checkManager()
			elif it._type and it._type != type(obj):
				raise ValueError("Current serialization does not support heterogeneous types")
			self.__index.append(self.__rawMemory.writeEnd())
			self._writer.write(obj, self._protocol)
		it = ISimpleWriteIterator(write)
		it._type = None
		return it

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
		self._type_id = None
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
		if not self._native:
			self._type_id = self._manager.reader.readTypeAux(headProto)
		self.__checkManager()

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
		if self._native:
			object._protocol = obs
		else:
			object._protocol = IObjectProtocol(obs)
		object.__rawMemory = obs
		object.__readOnly = True
		return object

	def __checkManager(self):
		if self._writer and not self._native:
			self._type_id = self._writer.getId()
		else:
			if self._type_id:
				self._reader = self._manager.reader.getReader(self._type_id)
				self._writer = self._manager.writer.getWriterByType(self._reader.getId())
				self._protocol = IObjectProtocol(self.__rawMemory)
			else:
				self._reader = self._manager.nativeReader
				self._writer = self._manager.nativeWriter
				self._protocol = self.__rawMemory

	@property
	def _elems(self):
		return self._elems_.value

	@_elems.setter
	def _elems(self, value):
		self._elems_.value = value

	@property
	def _type_id(self):
		value = self._type_id_.value
		if value == 0xFFFFF:
			return None
		return value

	@_type_id.setter
	def _type_id(self, value):
		if value is None:
			value = 0xFFFFF
		self._type_id_.value = value

	@property
	def _native(self):
		return self._native_.value

	@_native.setter
	def _native(self, value):
		self._native_.value = value
