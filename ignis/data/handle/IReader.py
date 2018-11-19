from .IEnumTypes import IEnumTypes
from ignis.data.IObjectProtocol import IObjectProtocol

class IReader:
	class __IReaderType:
		def __init__(self, tp, read):
			self.__type = tp
			self.read = read

		def getId(self):
			return self.__type

	def getProtocol(self, transport):
		return IObjectProtocol(transport)

	def __init__(self) -> None:
		self.__methods = dict()
		self.__methods[IEnumTypes.I_VOID] = self.__IReaderType(None, self.readVoid)
		self.__methods[IEnumTypes.I_BOOL] = self.__IReaderType(bool, self.readBool)
		self.__methods[IEnumTypes.I_I08] = self.__IReaderType(int, self.readByte)
		self.__methods[IEnumTypes.I_I16] = self.__IReaderType(int, self.readI16)
		self.__methods[IEnumTypes.I_I32] = self.__IReaderType(int, self.readI32)
		self.__methods[IEnumTypes.I_I64] = self.__IReaderType(int, self.readI64)
		self.__methods[IEnumTypes.I_DOUBLE] = self.__IReaderType(float, self.readDouble)
		self.__methods[IEnumTypes.I_STRING] = self.__IReaderType(str, self.readString)
		self.__methods[IEnumTypes.I_LIST] = self.__IReaderType(list, self.readList)
		self.__methods[IEnumTypes.I_SET] = self.__IReaderType(set, self.readSet)
		self.__methods[IEnumTypes.I_MAP] = self.__IReaderType(map, self.readMap)
		self.__methods[IEnumTypes.I_PAIR] = self.__IReaderType(tuple, self.readPair)
		self.__methods[IEnumTypes.I_BINARY] = self.__IReaderType(bytearray, self.readBinary)
		self.__methods[IEnumTypes.I_PAIR_LIST] = self.__IReaderType(list, self.readPairList)

	def getReader(self, tp):
		if tp in self.__methods:
			return self.__methods[tp]
		raise NotImplementedError("IReaderType not implemented for id " + str(tp))

	def readTypeAux(self, protocol):
		return protocol.readByte()

	def readSizeAux(self, protocol):
		return protocol.readI64()

	def readVoid(self, protocol):
		pass

	def readBool(self, protocol):
		return protocol.readBool()

	def readByte(self, protocol):
		return protocol.readByte()

	def readI16(self, protocol):
		return protocol.readI16()

	def readI32(self, protocol):
		return protocol.readI32()

	def readI64(self, protocol):
		return protocol.readI64()

	def readDouble(self, protocol):
		return protocol.readDouble()

	def readString(self, protocol):
		return protocol.readString()

	def readList(self, protocol):
		object = list()
		size = self.readSizeAux(protocol)
		reader = self.getReader(self.readTypeAux(protocol))
		for i in range(0, size):
			object.append(reader.read(protocol))
		return object

	def readSet(self, protocol):
		object = set()
		size = self.readSizeAux(protocol)
		reader = self.getReader(self.readTypeAux(protocol))
		for i in range(0, size):
			object.add(reader.read(protocol))
		return object

	def readMap(self, protocol):
		object = dict()
		size = self.readSizeAux(protocol)
		keyReader = self.getReader(self.readTypeAux(protocol))
		valueReader =self.getReader(self.readTypeAux(protocol))
		for i in range(0, size):
			object[keyReader.read(protocol)] = valueReader.read(protocol)
		return object

	def readPair(self, protocol):
		firstReader = self.getReader(self.readTypeAux(protocol))
		secondReader = self.getReader(self.readTypeAux(protocol))
		return firstReader.read(protocol), secondReader.read(protocol)

	def readBinary(self, protocol):
		object = bytearray()
		size = self.readSizeAux(protocol)
		for i in range(0, size):
			object.append(protocol.readByte())
		return object

	def readPairList(self, protocol):
		object = list()
		size = self.readSizeAux(protocol)
		firstReader = self.getReader(self.readTypeAux(protocol))
		secondReader = self.getReader(self.readTypeAux(protocol))
		for i in range(0, size):
			object.append((firstReader.read(protocol), secondReader.read(protocol)))
		return object
