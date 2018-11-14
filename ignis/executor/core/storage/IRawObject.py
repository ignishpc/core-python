from ignis.executor.core.storage.IObject import IObject
from ignis.executor.core.storage.iterator.ITransportIterator import IReadTransportIterator, IWriteTransportIterator
from ignis.executor.core.storage.iterator.ICoreIterator import readToWrite
from ignis.data.IZipTransport import IZipTransport

class IRawObject(IObject):

	def __init__(self, transport, compression, manager):
		self._transport = transport
		self._compression = compression
		self._manager = manager
		self._type = None
		self._elems = 0

	def readIterator(self):
		return IReadTransportIterator(self.__transport, self._manager, self._elems)

	def writeIterator(self):
		def written():
			self._elems = self._elems + 1

		return IWriteTransportIterator(self.__transport, self._manager, lambda: written)

	def read(self, trans):
		self.clear()
		dataTransport = IZipTransport(trans)
		self.__readHeader(dataTransport)
		while True:
			buffer = dataTransport.read(256)
			if not buffer:
				break
			self.__transport.write(buffer)
		self.__transport.flush()

	def write(self, trans, compression):
		dataTransport = IZipTransport(trans)
		self.__writeHeader(dataTransport)
		while True:
			buffer = self.__transport.read(256)
			if not buffer:
				break
				dataTransport.write(buffer)
		dataTransport.flush()

	def copyFrom(self, source):
		readToWrite(source.readIterator(), self.writeIterator())

	def moveFrom(self, source):
		self.copyFrom(source)

	def __len__(self):
		return self._elems

	def clear(self):
		self._type = None
		self._elems = 0

	def __readHeader(self, transport):
		pass

	def __writeHeader(self, transport):
		pass
