from ignis.executor.core.io.IEnumTypes import IEnumTypes


class IReader:
	__types = dict()

	@classmethod
	def __setitem__(cls, key, value):
		cls.__types[key] = value

	@classmethod
	def __getitem__(cls, key):
		return cls.__types[key]

	@classmethod
	def __delitem__(cls, key):
		del cls.__types[key]

	def __init__(self):
		self.__types = IReader.__types.copy()

	def _getReaderType(self, id):
		try:
			return self.__types[id]
		except KeyError as ex:
			raise NotImplementedError("IWriterType not implemented for id" + str(id))

	def _readSizeAux(self, protocol):
		return protocol.readI64()

	def _readTypeAux(self, protocol):
		return protocol.readByte()

	def read(self, protocol):
		readerType = self.getReaderType(self._readTypeAux(protocol))
		return readerType.read(protocol)


class IReaderType:

	def __init__(self, read):
		self.read = read


IReader[IEnumTypes.I_VOID] = IReaderType(lambda reader, protocol: None)
IReader[IEnumTypes.I_BOOL] = IReaderType(lambda reader, protocol: protocol.readBool())
IReader[IEnumTypes.I_I08] = IReaderType(lambda reader, protocol: protocol.readByte())
IReader[IEnumTypes.I_I16] = IReaderType(lambda reader, protocol: protocol.readI16())
IReader[IEnumTypes.I_I32] = IReaderType(lambda reader, protocol: protocol.readI32())
IReader[IEnumTypes.I_I64] = IReaderType(lambda reader, protocol: protocol.readI64())
IReader[IEnumTypes.I_DOUBLE] = IReaderType(lambda reader, protocol: protocol.readDouble())
IReader[IEnumTypes.I_STRING] = IReaderType(lambda reader, protocol: protocol.readString())


def __readList(reader, protocol):
	obj = list()
	size = reader.readSizeAux(protocol)
	readerType = reader._getReaderType(reader.readTypeAux(protocol))
	for i in range(0, size):
		obj.append(readerType.read(reader, protocol))
	return obj


def __readSet(reader, protocol):
	obj = set()
	size = reader.readSizeAux(protocol)
	readerType = reader._getReaderType(reader.readTypeAux(protocol))
	for i in range(0, size):
		obj.add(readerType.read(reader, protocol))
	return obj


def __readMap(reader, protocol):
	obj = dict()
	size = reader.readSizeAux(protocol)
	keyReader = reader._getReaderType(reader.readTypeAux(protocol))
	valueReader = reader._getReaderType(reader.readTypeAux(protocol))
	for i in range(0, size):
		obj[keyReader.read(reader, protocol)] = valueReader.read(reader, protocol)
	return obj


def __readPair(reader, protocol):
	firstReader = reader._getReaderType(reader.readTypeAux(protocol))
	secondReader = reader._getReaderType(reader.readTypeAux(protocol))
	return firstReader.read(reader, protocol), secondReader.read(reader, protocol)


def __readBinary(reader, protocol):
	obj = bytearray()
	size = reader.readSizeAux(protocol)
	for i in range(0, size):
		obj.append(protocol.readByte())
	return obj


def __readPairList(reader, protocol):
	obj = list()
	size = reader.readSizeAux(protocol)
	firstReader = reader._getReaderType(reader.readTypeAux(protocol))
	secondReader = reader._getReaderType(reader.readTypeAux(protocol))
	for i in range(0, size):
		obj.append((firstReader.read(reader, protocol), secondReader.read(reader, protocol)))
	return obj


IReader[IEnumTypes.I_LIST] = IReaderType(__readList)
IReader[IEnumTypes.I_SET] = IReaderType(__readSet)
IReader[IEnumTypes.I_MAP] = IReaderType(__readMap)
IReader[IEnumTypes.I_PAIR] = IReaderType(__readPair)
IReader[IEnumTypes.I_BINARY] = IReaderType(__readBinary)
IReader[IEnumTypes.I_PAIR_LIST] = IReaderType(__readPairList)
