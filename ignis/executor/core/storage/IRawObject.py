from .IObject import IObject
from .iterator.ICoreIterator import readToWrite
from .iterator.ISimpleIterator import ISimpleReadIterator, ISimpleWriteIterator
from ignis.data.IZlibTransport import IZlibTransport
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
			return it._elems < self._elems

		def next(it):
			it._elems += 1
			return self._reader.read(self._protocol)

		return ISimpleReadIterator(next=next, hasNext=hasNext)

	def writeIterator(self):
		def write(it, obj):
			self._elems += 1
			if self._writer is None or self._reader is None:
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

		return ISimpleWriteIterator(write)

	def read(self, trans):
		self.clear()
		dataTransport = IZlibTransport(trans)
		self.__readHeader(dataTransport)
		while True:
			buffer = dataTransport.read(256)
			if not buffer:
				break
			self._transport.write(buffer)
		self._transport.flush()

	def write(self, trans, compression):
		dataTransport = IZlibTransport(trans)
		self.__writeHeader(dataTransport)
		while True:
			buffer = self._transport.read(256)
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
		self._elems = 0
		self._writer = None
		self._reader = None

	def __readHeader(self, transport):
		headProto = IObjectProtocol(transport)
		self._native = headProto.readBool()
		self._manager.reader.readTypeAux(headProto)
		self._elems = self._manager.reader.readSizeAux(headProto)
		if self._native:
			self._reader = self._manager.nativeReader
			self._writer = self._manager.nativeWriter
			self._protocol = self._transport
		else:
			self._reader = self._manager.reader.getReader(self._manager.reader.readTypeAux(headProto))
			self._writer = self._manager.writer.getWriterByType(self._reader.getId())
			self._protocol = IObjectProtocol(self._transport)

	def __writeHeader(self, transport):
		headProto = IObjectProtocol(transport)
		headProto.writeBool(self._native)
		self._manager.writer.getWriter(list()).writeType(headProto)
		self._manager.writer.writeSizeAux(self._elems, headProto)
		if not self._native:
			self._writer.writeType(headProto)
