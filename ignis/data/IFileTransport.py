from thrift.transport.TTransport import TTransportBase


class IFileTransport(TTransportBase):

	def __init__(self, buffer1, buffer2, sync, blockSize=50 * 1024 * 1024):
		file1 = open(buffer1, mode='wb+')
		file2 = open(buffer2, mode='wb+')
		self.__buffer = (file1, file2)
		self.__syncTrans = sync
		self.__blockSize = blockSize
		self.__init = False
		self.__writeBytes = blockSize

	def isOpen(self):
		self.__syncTrans.isOpen()

	def open(self):
		if not self.isOpen():
			self.__syncTrans.open()

	def close(self):
		self.flush()
		self.__buffer[0].close()
		self.__buffer[1].close()
		self.__syncTrans.close()

	def read(self, sz):
		readBytes = self.__buffer[0].read(sz)
		if len(readBytes) == 0:
			self.__swapRead()
			readBytes = self.__buffer[0].read(sz)
			if not self.__init:
				self.__init = True
				self.__swapRead()
				readBytes = self.__buffer[0].read(sz)
		return readBytes

	def write(self, buf):
		sz = len(buf)
		pos = 0
		while self.__writeBytes + sz > self.__blockSize:
			avail = self.__blockSize - self.__writeBytes
			self.__buffer[1].write(buf[pos:avail])
			sz -= avail
			pos += avail
			self.__swapWrite()
		self.__buffer[1].write(buf[pos:sz])
		self.__writeBytes += sz

	def flush(self):
		if self.__writeBytes > 0:
			self.__swapWrite()

	def __sync(self):
		self.__syncTrans.write(b'0')
		self.__syncTrans.flush()
		self.__syncTrans.read(1)

	def __swapRead(self):
		self.__sync()
		self.__buffer = self.__buffer[::-1]
		self.__buffer[0].seek(0)

	def __swapWrite(self):
		self.__buffer[1].flush()
		self.__sync()
		self.__buffer = self.__buffer[::-1]
		self.__buffer[1].truncate(0)
		self.__writeBytes = 0
