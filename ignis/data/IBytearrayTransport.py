from thrift.transport.TTransport import TTransportBase


class IBytearrayTransport(TTransportBase):

	def __init__(self, array = None):
		if array is None:
			array = bytearray()
		self.__array = array
		self.__read = 0

	def resetRead(self, read=0):
		self.__read = read

	def clear(self):
		array = bytearray()
		self.__read = 0

	def isOpen(self):
		return True

	def open(self):
		pass

	def close(self):
		pass

	def read(self, sz):
		if self.__read + sz > len(self.__array):
			raise EOFError()
		oldRead = self.__read
		self.__read += sz
		return self.__array[oldRead:self.__read]

	def readAll(self, sz):
		return self.read(sz)

	def write(self, buf):
		self.__array.append(buf)

	def flush(self):
		pass
