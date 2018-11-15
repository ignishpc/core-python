from .IObject import IObject
from .iterator.ICoreIterator import readToWrite
from .iterator.ISimpleIterator import IBasicReadIterator, IBasicWriteIterator
from ignis.data.IZipTransport import IZipTransport
from ignis.data.IObjectProtocol import IObjectProtocol


class IRawObject(IObject):

	def __init__(self, transport, compression, manager, native):
		self._transport = transport
		self._compression = compression
		self._manager = manager
		self._native = native
		self._reader = None
		self._writer = None
		self._protocol = None
		self._elems = 0

	def readIterator(self):
		def hasNext(it):
			return it.__elems < self._elems

		def next(it):
			it.__elems += 1
			return self._reader.read(self._protocol)


	def writeIterator(self):
		def write(it, obj):
			self._elems += 1
			if self._writer is None:
				if self._native:
					self._writer = self._manager.nativeWriter
					self._reader = self._manager.nativeReader
					self._protocol = self._transport
				else:
					self._writer = self._manager.writer.getWriter(obj)
					self._reader = self._manager.reader.getReader(self._writer.getId())
					self._protocol = IObjectProtocol(self._transport)
			elif self.__type != type(obj):
				raise ValueError("Current serialization does not support heterogeneous types")
			self.__type = type(obj)
			self._writer.write(obj, self._protocol)

		return IBasicWriteIterator(write)

	def read(self, trans):
		self.clear()
		dataTransport = IZipTransport(trans)
		self.__readHeader(dataTransport)
		while True:
			buffer = dataTransport.read(256)
			if not buffer:
				break
			self.__transport.write(buffer)
		self.__transport.flush()

	def write(self, trans, compression):
		dataTransport = IZipTransport(trans)
		self.__writeHeader(dataTransport)
		while True:
			buffer = self.__transport.read(256)
			if not buffer:
				break
				dataTransport.write(buffer)
		dataTransport.flush()

	def copyFrom(self, source):
		readToWrite(source.readIterator(), self.writeIterator())

	def moveFrom(self, source):
		self.copyFrom(source)

	def __len__(self):
		return self._elems

	def clear(self):
		self._type = None
		self._elems = 0

	def __readHeader(self, transport):
		headProto = IObjectProtocol(transport)
		self._native = headProto.readBool()
		self._elems = headProto.readI64()
		if self._native:
			self._reader = self._manager.nativeReader
			self._writer = self._manager.nativeWriter
			self._protocol = self._transport
		else:
			self._reader = self._manager.reader.readTypeAux(headProto)
			self._writer = self._manager.writer.getWriter(self._reader.getId())
			self._protocol = IObjectProtocol(self._transport)

	def __writeHeader(self, transport):
		headProto = IObjectProtocol(transport)
		headProto.writeBool(self._native)
		headProto.writeI64(self._elems)
		if self._native:
			self._writer.writeType(headProto)