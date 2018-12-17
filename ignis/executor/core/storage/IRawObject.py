from .IObject import IObject
from .iterator.IRawIterator import readToWrite, IRawReadIterator, IRawWriteIterator
from ignis.data.IZlibTransport import IZlibTransport
from ignis.data.IObjectProtocol import IObjectProtocol
from ignis.data.IBytearrayTransport import IBytearrayTransport


class IRawObject(IObject):
	TYPE = "raw"

	def __init__(self, transport, manager, native):
		self._transport = transport
		self._manager = manager
		self._native = native
		self._reader = None
		self._writer = None
		self._protocol = None
		self._elems = 0

	def readIterator(self):
		self._flush()
		return IRawReadIterator(self)

	def writeIterator(self):
		return IRawWriteIterator(self)

	def read(self, trans):
		self.clear()
		dataTransport = IZlibTransport(trans)
		self.__readHeader(dataTransport)
		while True:
			buffer = dataTransport.read(256)
			if not buffer:
				break
			self._transport.write(buffer)

	def write(self, trans, compression):
		self._flush()
		dataTransport = IZlibTransport(trans)
		self.__writeHeader(dataTransport)
		while True:
			buffer = self._transport.read(256)
			if not buffer:
				break
			dataTransport.write(buffer)
		dataTransport.flush()

	def copyTo(self, target):
		if isinstance(target, IRawObject) and target._native == self._native:
			self._flush()
			if len(target) == 0:
				aux = IBytearrayTransport()
				self.__writeHeader(aux)
				target.__readHeader(aux)
			else:
				target._elems += self._elems
			while True:
				buffer = self._transport.read(256)
				if not buffer:
					break
				target._transport.write(buffer)
		else:
			readToWrite(self.readIterator(), target.writeIterator())

	def copyFrom(self, source):
		if isinstance(source, IRawObject):
			source.copyTo(self)
		else:
			readToWrite(source.readIterator(), self.writeIterator())

	def moveFrom(self, source):
		self.copyFrom(source)
		source.clear()

	def __len__(self):
		return self._elems

	def clear(self):
		self._flush()
		self._elems = 0
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
			self._protocol = self._transport
		else:
			self._reader = self._manager.reader.getReader(self._manager.reader.readTypeAux(headProto))
			self._writer = self._manager.writer.getWriterByType(self._reader.getId())
			self._protocol = IObjectProtocol(self._transport)

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

	def _flush(self):
		if self._elems > 0:
			self._transport.flush()
