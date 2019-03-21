from .IEnumTypes import IEnumTypes
from ignis.data.IObjectProtocol import IObjectProtocol
import numpy


class IWriter:
	class __IWriterType:
		def __init__(self, tp, write):
			self.__type = tp
			self.write = write

		def getId(self):
			return self.__type

		def writeType(self, protocol):
			protocol.writeByte(self.__type)

	def getProtocol(self, transport):
		return IObjectProtocol(transport)

	def __init__(self) -> None:
		self.__methods = dict()
		self.__methods[type(None)] = self.__IWriterType(IEnumTypes.I_VOID, self.writeVoid)
		self.__methods[bool] = self.__IWriterType(IEnumTypes.I_BOOL, self.writeBool)
		self.__methods[int] = self.__IWriterType(IEnumTypes.I_I64, self.writeI64)
		self.__methods[float] = self.__IWriterType(IEnumTypes.I_DOUBLE, self.writeDouble)
		self.__methods[str] = self.__IWriterType(IEnumTypes.I_STRING, self.writeString)
		self.__methods[list] = self.__IWriterType(IEnumTypes.I_LIST, self.writeList)
		self.__methods[set] = self.__IWriterType(IEnumTypes.I_SET, self.writeSet)
		self.__methods[dict] = self.__IWriterType(IEnumTypes.I_MAP, self.writeMap)
		self.__methods[tuple] = self.__IWriterType(IEnumTypes.I_PAIR, self.writePair)
		self.__methods[bytes] = self.__IWriterType(IEnumTypes.I_BINARY, self.writeBytes)
		self.__methods[bytearray] = self.__IWriterType(IEnumTypes.I_BINARY, self.writeBytes)
		self.__methods[numpy.ndarray] = self.__IWriterType(IEnumTypes.I_LIST, self.writeList)

	def getWriterByType(self, tp):
		if tp in self.__methods:
			return self.__methods[tp]
		raise NotImplementedError("IWriterType not implemented for " + str(tp.__name__))

	def getWriter(self, object):
		return self.getWriterByType(type(object))

	def writeSizeAux(self, size, protocol):
		protocol.writeI64(size)

	def writeVoid(self, object, protocol):
		pass

	def writeBool(self, object, protocol):
		protocol.writeBool(object)

	def writeByte(self, object, protocol):
		protocol.writeByte(object)

	def writeI16(self, object, protocol):
		protocol.writeI16(object)

	def writeI32(self, object, protocol):
		protocol.writeI32(object)

	def writeI64(self, object, protocol):
		protocol.writeI64(object)

	def writeDouble(self, object, protocol):
		protocol.writeDouble(object)

	def writeString(self, object, protocol):
		protocol.writeString(object)

	def writeList(self, object, protocol):
		size = len(object)
		self.writeSizeAux(size, protocol)
		if size == 0:
			writer = self.getWriter(None)
		else:
			writer = self.getWriter(object[0])
		writer.writeType(protocol)
		for elem in object:
			writer.write(elem, protocol)

	def writeSet(self, object, protocol):
		size = len(object)
		self.writeSizeAux(size, protocol)
		if size == 0:
			writer = self.getWriter(None)
		else:
			writer = self.getWriter(next(iter(object)))
		writer.writeType(protocol)
		for elem in object:
			writer.write(elem, protocol)

	def writeMap(self, object, protocol):
		size = len(object)
		self.writeSizeAux(size, protocol)
		if size == 0:
			keyWriter = self.getWriter(None)
			valueWriter = self.getWriter(None)
		else:
			entry = next(iter(object))
			keyWriter = self.getWriter(entry[0])
			valueWriter = self.getWriter(entry[1])
		keyWriter.writeType(protocol)
		valueWriter.writeType(protocol)
		for key, value in object.items():
			keyWriter.write(key, protocol)
			valueWriter.write(value, protocol)

	def writePair(self, object, protocol):
		if len(object) != 2:
			raise NotImplementedError("IWriterType not implemented for len(tuple) != 2")
		firstReader = self.getWriter(object[0])
		secondReader = self.getWriter(object[1])
		firstReader.writeType(protocol)
		secondReader.writeType(protocol)
		firstReader.write(object[0], protocol)
		secondReader.write(object[1], protocol)

	def writeBytes(self, object, protocol):
		self.writeSizeAux(len(object), protocol)
		for b in object:
			protocol.writeByte(b)
