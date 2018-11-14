from ignis.data.IObjectProtocol import IObjectProtocol


class IReadTransportIterator:

	def __init__(self, rawObject):
		self.__rawObject = rawObject
		self.__protocol = IObjectProtocol(rawObject._transport)
		self.__reader = None#TODO
		self.__elems = 0

	def next(self):
		self.__elems = self.__elems + 1
		return self.__reader.read(self.__protocol)

	def hasNext(self):
		return self.__elems > 0


class IWriteTransportIterator:

	def __init__(self, transport, writeType, written):
		self.__transport = transport
		self.__protocol = IObjectProtocol(transport)
		self.__writer = writeType
		self.__written = written

	def write(self, object):
		self.__writer.write(object, self.__protocol)
		self.written()
