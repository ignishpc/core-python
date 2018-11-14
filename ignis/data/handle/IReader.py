from ignis.data.handle.IEnumTypes import IEnumTypes


class IReader:
	class __IReaderType:
		def __init__(self, read):
			self.read = read

	def __init__(self) -> None:
		self.__methods = dict()
		self.__methods[IEnumTypes.I_VOID] = self.__IReaderType(self.readVoid)
		self.__methods[IEnumTypes.I_BOOL] = self.__IReaderType(self.readBool)
		self.__methods[IEnumTypes.I_I08] = self.__IReaderType(self.readByte)
		self.__methods[IEnumTypes.I_I16] = self.__IReaderType(self.readI16)
		self.__methods[IEnumTypes.I_I32] = self.__IReaderType(self.readI32)
		self.__methods[IEnumTypes.I_I64] = self.__IReaderType(self.readI64)
		self.__methods[IEnumTypes.I_DOUBLE] = self.__IReaderType(self.readDouble)
		self.__methods[IEnumTypes.I_STRING] = self.__IReaderType(self.readString)
		self.__methods[IEnumTypes.I_LIST] = self.__IReaderType(self.readList)
		self.__methods[IEnumTypes.I_SET] = self.__IReaderType(self.readSet)
		self.__methods[IEnumTypes.I_MAP] = self.__IReaderType(self.readMap)
		self.__methods[IEnumTypes.I_PAIR] = self.__IReaderType(self.readPair)
		self.__methods[IEnumTypes.I_BINARY] = self.__IReaderType(self.readBinary)
		self.__methods[IEnumTypes.I_PAIR_LIST] = self.__IReaderType(self.readPairList)

	def readTypeAux(self, protocol):
		type = protocol.readI8()
		if type in self.__methods:
			return self.methods[type]
		raise NotImplementedError("IReaderType not implemented for id " + type)

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
		reader = self.readTypeAux(protocol)
		for i in range(0, size):
			object.append(reader.read())
		return object

	def readSet(self, protocol):
		object = set()
		size = self.readSizeAux(protocol)
		reader = self.readTypeAux(protocol)
		for i in range(0, size):
			object.add(reader.read())
		return object

	def readMap(self, protocol):
		object = dict()
		size = self.readSizeAux(protocol)
		keyReader = self.readTypeAux(protocol)
		valueReader = self.readTypeAux(protocol)
		for i in range(0, size):
			object[keyReader.read()] = valueReader.read()
		return object

	def readPair(self, protocol):
		firstReader = self.readTypeAux(protocol)
		secondReader = self.readTypeAux(protocol)
		return (firstReader.read(), secondReader.read())

	def readBinary(self, protocol):
		object = bytearray()
		size = self.readSizeAux(protocol)
		for i in range(0, size):
			object.append(protocol.readByte())
		return object


	def readPairList(self, protocol):
		object = list()
		size = self.readSizeAux(protocol)
		firstReader = self.readTypeAux(protocol)
		secondReader = self.readTypeAux(protocol)
		for i in range(0, size):
			object.append((firstReader.read(), secondReader.read()))
		return object
