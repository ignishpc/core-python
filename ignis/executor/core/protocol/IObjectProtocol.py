from thrift.protocol.TCompactProtocol import TCompactProtocol, CompactType, makeZigZag
from struct import pack, unpack
from ignis.executor.core.io.IWriter import IWriter
from ignis.executor.core.io.INativeWriter import INativeWriter
from ignis.executor.core.io.IReader import IReader
from ignis.executor.core.io.INativeReader import INativeReader


class IObjectProtocol(TCompactProtocol):

	def __init__(self, trans):
		TCompactProtocol.__init__(self, trans)

	def writeBool(self, bool):
		if bool:
			self._TCompactProtocol__writeByte(CompactType.TRUE)
		else:
			self._TCompactProtocol__writeByte(CompactType.FALSE)

	def readBool(self):
		return self._TCompactProtocol__readByte() == CompactType.TRUE

	writeByte = TCompactProtocol._TCompactProtocol__writeByte
	writeI16 = TCompactProtocol._TCompactProtocol__writeI16

	def writeI32(self, i32):
		self._TCompactProtocol__writeVarint(makeZigZag(i32, 32))

	def writeI64(self, i64):
		self._TCompactProtocol__writeVarint(makeZigZag(i64, 64))

	def writeDouble(self, dub):
		self.trans.write(pack('<d', dub))

	writeBinary = TCompactProtocol._TCompactProtocol__writeBinary

	readByte = TCompactProtocol._TCompactProtocol__readByte
	readI16 = TCompactProtocol._TCompactProtocol__readZigZag
	readI32 = TCompactProtocol._TCompactProtocol__readZigZag
	readI64 = TCompactProtocol._TCompactProtocol__readZigZag

	def readDouble(self):
		buff = self.trans.readAll(8)
		val, = unpack('<d', buff)
		return val

	readBinary = TCompactProtocol._TCompactProtocol__readBinary

	def readObject(self):
		native = self.readBool()
		reader = IReader()
		if native:
			nativeReader = INativeReader()
			header = self.readBool()
			if header:
				elems = reader._readSizeAux(self)
				array = list()
				for i in range(0, elems):
					array.append(nativeReader.read(self))
				return array
			else:
				return nativeReader.read(self)
		else:
			return reader.read(self)

	def writeObject(self, obj, native=False, listHeader=False):
		self.writeBool(native)
		writer = IWriter()
		if native:
			nativeWriter = INativeWriter()
			isList = type(obj) == list
			self.writeBool(isList and listHeader)  # Header
			if isList and listHeader:
				writer._writeSizeAux(self, len(obj))
				for i in range(0, len(obj)):
					nativeWriter.write(self, obj[i])
			else:
				nativeWriter.write(self, obj)
		else:
			writer.write(self, obj)
