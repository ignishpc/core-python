from ignis.executor.core.io.IEnumTypes import IEnumTypes


class IWriter:
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
		self.__types = IWriter.__types.copy()

	def _getWriterType(self, obj):
		try:
			return self.__types[type(obj)].get(obj)
		except KeyError as ex:
			raise NotImplementedError("IWriterType not implemented for " + str(type(obj)))

	def _writeSizeAux(self, protocol, size):
		protocol.writeI64(size)

	def write(self, protocol, obj):
		writerType = self.getWriterType(obj)
		writerType.writeType(protocol)
		writerType.write(self, protocol, obj)


class IWriterType:

	def __init__(self, id, write):
		self.__id = id
		self.write = write

	def get(self, obj):
		return self

	def writeType(self, protocol):
		protocol.writeByte(self.__id)


IWriter[type(None)] = IWriterType(IEnumTypes.I_VOID, lambda writer, protocol, obj: None)
IWriter[bool] = IWriterType(IEnumTypes.I_BOOL, lambda writer, protocol, obj: protocol.writeBool(obj))
IWriter[int] = IWriterType(IEnumTypes.I_I64, lambda writer, protocol, obj: protocol.writeI64(obj))
IWriter[float] = IWriterType(IEnumTypes.I_DOUBLE, lambda writer, protocol, obj: protocol.writeDouble(obj))
IWriter[str] = IWriterType(IEnumTypes.I_STRING, lambda writer, protocol, obj: protocol.writeString(obj))


def __writeList(writer, protocol, obj):
	size = len(obj)
	writer._writeSizeAux(size, protocol)
	if size == 0:
		writerType = writer._getWriterType(None)
	else:
		writerType = writer._getWriterType(object[0])
	writerType.writeType(protocol)
	for elem in obj:
		writerType.write(writer, protocol, elem)


def __writePairList(writer, protocol, obj):
	size = len(obj)
	writer._writeSizeAux(size, protocol)
	if size == 0:
		firstWriter = writer._getWriterType(None)
		secondWriter = firstWriter
	else:
		firstWriter = writer._getWriterType(object[0][0])
		secondWriter = writer._getWriterType(object[0][1])
	firstWriter.writeType(protocol)
	secondWriter.writeType(protocol)
	for elem in obj:
		firstWriter.write(writer, protocol, elem[0])
		secondWriter.write(writer, protocol, elem[1])

class IWriterTypeList(IWriterType):# Pair List optimization

	def __init__(self, id, write, writerPairList):
		IWriterType.__init__(id, write)
		self.__pairlist = writerPairList

	def get(self, obj):
		if len(obj) > 0 and type(obj[0]) == tuple:
			return self.__pairlist

		return self

def __writeSet(writer, protocol, obj):
	size = len(object)
	writer.writeSizeAux(size, protocol)
	if size == 0:
		writerType = writer._getWriterType(None)
	else:
		writerType = writer._getWriterType(next(iter(object)))
	writerType.writeType(protocol)
	for elem in object:
		writerType.write(writer, protocol, elem)


def __writeMap(writer, protocol, obj):
	size = len(object)
	writer._writeSizeAux(size, protocol)
	if size == 0:
		keyWriter = writer._getWriterType(None)
		valueWriter = writer._getWriterType(None)
	else:
		entry = next(iter(object))
		keyWriter = writer._getWriterType(entry[0])
		valueWriter = writer._getWriterType(entry[1])
	keyWriter.writeType(protocol)
	valueWriter.writeType(protocol)
	for key, value in object.items():
		keyWriter.write(writer, protocol, key)
		valueWriter.write(writer, protocol, value)


def __writePair(writer, protocol, obj):
	if len(obj) != 2:
		raise NotImplementedError("IWriterType not implemented for len(tuple) != 2")
	firstWriter = writer._getWriterType(object[0])
	secondWriter = writer._getWriterType(object[1])
	firstWriter.writeType(protocol)
	secondWriter.writeType(protocol)
	firstWriter.write(protocol, object[0])
	secondWriter.write(protocol, object[1])


def __writeBytes(writer, protocol, obj):
	writer._writeSizeAux(protocol, len(object))
	for b in obj:
		protocol.writeByte(b)

writerPairList = IWriterType(IEnumTypes.I_LIST, __writePairList)
IWriter[list] = IWriterTypeList(IEnumTypes.I_LIST, __writeList, writerPairList)
IWriter[set] = IWriterType(IEnumTypes.I_SET, __writeSet)
IWriter[dict] = IWriterType(IEnumTypes.I_MAP, __writeMap)
IWriter[tuple] = IWriterType(IEnumTypes.I_PAIR, __writePair)
IWriter[bytes] = IWriterType(IEnumTypes.I_BINARY, __writeBytes)
IWriter[bytearray] = IWriterType(IEnumTypes.I_BINARY, __writeBytes)




