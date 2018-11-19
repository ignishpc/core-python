from thrift.protocol.TCompactProtocol import TCompactProtocol, CompactType, makeZigZag
from struct import pack, unpack


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

	def readObject(self, manager):
		native = self.readBool()
		if native:
			header = self.readBool()
			if header:
				elems = manager.reader.readSizeAux(self)
				l = list()
				for i in range(0, elems):
					l.append(manager.nativeReader.read(self.trans))
				return l
			else:
				return manager.nativeReader.read(self.trans)
		else:
			return manager.reader.getReader(manager.reader.readTypeAux(self)).read(self)

	def writeObject(self, obj, manager, native, listHeader=False):
		self.writeBool(native)
		if native:
			isList = type(obj) == list
			self.writeBool(isList and listHeader)  # Header
			if isList and listHeader:
				manager.writer.writeSizeAux(len(obj), self)
				for i in range(0, len(obj)):
					manager.nativeWriter.write(obj[i], self.trans)
			else:
				manager.nativeWriter.write(obj, self.trans)
		else:
			writer = manager.writer.getWriter(obj)
			writer.writeType(self)
			writer.write(obj, self)
