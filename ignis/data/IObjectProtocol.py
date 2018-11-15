from thrift.protocol.TCompactProtocol import TCompactProtocol
from struct import pack, unpack

class IObjectProtocol(TCompactProtocol):

	def __init__(self, trans):
		TCompactProtocol.__init__(self, trans)

	def writeBool(self, bool):
		if bool:
			self._TCompactProtocol__writeByte(TCompactProtocol.CompactType.TRUE)
		else:
			self._TCompactProtocol__writeByte(TCompactProtocol.CompactType.FALSE)

	def readBool(self):
		return self._TCompactProtocol__readByte() == TCompactProtocol.CompactType.TRUE

	writeByte = TCompactProtocol._TCompactProtocol__writeByte
	writeI16 = TCompactProtocol._TCompactProtocol__writeI16

	def writeI32(self, i32):
		self._TCompactProtocol__writeVarint(self.makeZigZag(i32, 32))

	def writeI64(self, i64):
		self._TCompactProtocol__writeVarint(self.makeZigZag(i64, 64))

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