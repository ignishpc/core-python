from ignis.data.handle.IEnumTypes import IEnumTypes


class IWriter:
	class __IWriterType:
		def __init__(self, _type, write):
			self.__type = _type
			self.write = write

		def writeType(self, protocol):
			protocol.writeByte(type)

	def __init__(self) -> None:
		self.__methods = dict()
		self.__methods[type(None)] = self.__IWriterType(IEnumTypes.I_VOID, self._writeVoid)
		self.__methods[type(bool)] = self.__IWriterType(IEnumTypes.I_BOOL, self._writeBool)
		self.__methods[type(int)] = self.__IWriterType(IEnumTypes.I_I64, self.writeI64)
		self.__methods[type(float)] = self.__IWriterType(IEnumTypes.I_DOUBLE, self.writeDouble)
		self.__methods[type(str)] = self.__IWriterType(IEnumTypes.I_STRING, self.writeString)
		self.__methods[type(list)] = self.__IWriterType(IEnumTypes.I_LIST, self.writeList)
		self.__methods[type(set)] = self.__IWriterType(IEnumTypes.I_SET, self.writeSet)
		self.__methods[type(dict)] = self.__IWriterType(IEnumTypes.I_MAP, self.writeMap)
		self.__methods[type(())] = self.__IWriterType(IEnumTypes.I_PAIR, self.writePair)
		self.__methods[type(bytes)] = self.__IWriterType(IEnumTypes.I_BINARY, self.writeBytes)
		self.__methods[type(bytearray)] = self.__IWriterType(IEnumTypes.I_BINARY, self.writeBytes)

	def getTypeAux(self, object):
		if type(object) in self.__methods:
			return self.methods[type(object)]
		raise NotImplementedError("IWriterType not implemented for " + str(type(object).__name__))

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
		self.writeSizeAux(size)
		if size == 0:
			writer = self.getTypeAux(None)
		else:
			writer = self.getTypeAux(object[0])
		writer.writeType(protocol)
		for elem in object:
			writer.write(elem)

	def writeSet(self, object, protocol):
		size = len(object)
		self.writeSizeAux(size)
		if size == 0:
			writer = self.getTypeAux(None)
		else:
			writer = self.getTypeAux(next(iter(object)))
		writer.writeType(protocol)
		for elem in object:
			writer.write(elem)

	def writeMap(self, object, protocol):
		size = len(object)
		self.writeSizeAux(size)
		if size == 0:
			keyWriter = self.getTypeAux(None)
			valueWriter = self.getTypeAux(None)
		else:
			entry = next(iter(object))
			keyWriter = self.getTypeAux(entry[0])
			valueWriter = self.getTypeAux(entry[1])
		keyWriter.writeType(protocol)
		valueWriter.writeType(protocol)
		for key, value in object.items():
			keyWriter.write(key)
			valueWriter.write(value)

	def writePair(self, object, protocol):
		if len(object):
			raise NotImplementedError("IWriterType not implemented for len(tuple) > 2")
		firstReader = self.getTypeAux(object[0])
		secondReader = self.getTypeAux(object[1])
		firstReader.writeType(protocol)
		secondReader.writeType(protocol)
		firstReader.write(object[0])
		secondReader.write(object[1])

	def writeBytes(self, object, protocol):
		self.writeSizeAux(len(object))
		for b in object:
			protocol.writeByte(b)
