from thrift.transport.TTransport import TTransportBase, TTransportException
from enum import Enum
import sys
import mmap


class ISharedMemoryBuffer(TTransportBase):
	"""
	A shared memory buffer is a transport that simply reads from and writes to an
	in memory buffer. Anytime you call write on it, the data is simply placed
	into a buffer, and anytime you call read, data is read from that buffer.

	The buffers are allocated using shared memory.
	"""

	defaultSize = 1024

	class MemoryPolicy(Enum):
		"""
		This enum specifies how a ISharedMemoryBuffer should treat
		memory passed to it via constructors or resetBuffer.

		OBSERVE:
		ISharedMemoryBuffer will simply store a pointer to the memory.
		It is the callers responsibility to ensure that the pointer
		remains valid for the lifetime of the ISharedMemoryBuffer,
		and that it is properly cleaned up.
		Note that no data can be written to observed buffers.

		COPY:
		ISharedMemoryBuffer will make an internal copy of the buffer.
		The caller has no responsibilities.

		TAKE_OWNERSHIP:
		ISharedMemoryBuffer will become the "owner" of the buffer,
		and will be responsible for freeing it.
		The memory must have been allocated with malloc.
		"""
		OBSERVE = 1
		COPY = 2
		TAKE_OWNERSHIP = 3

	def __init__(self, sz=None, buf=None, policy=MemoryPolicy.OBSERVE):
		"""
		Construct a ISharedMemoryBuffer

		@param buf    The initial contents of the buffer.
				   ISharedMemoryBuffer will not write to it if policy == OBSERVE.
		@param sz     The size of @c buf.
		@param policy See @link MemoryPolicy @endlink .
		"""
		if buf is None:
			if sz is None:
				self.__initCommon(None, ISharedMemoryBuffer.defaultSize, True, 0)
			else:
				self.__initCommon(None, sz, True, 0)
		else:
			if policy == MemoryPolicy.OBSERVE or MemoryPolicy.TAKE_OWNERSHIP:
				self.__initCommon(buf, sz, policy == MemoryPolicy.TAKE_OWNERSHIP, sz)
			elif policy == MemoryPolicy.COPY:
				self.__initCommon(None, sz, True, 0)
				self.write(buf, sz)
			else:
				raise TTransportException(message="Invalid MemoryPolicy for ISharedMemoryBuffer")

	def isOpen(self):
		return True

	def peek(self):
		return self.__rBase_ < self.__Base_

	def open(self):
		pass

	def close(self):
		pass

	def getBuffer(self):
		return self.__buffer

	def resetBuffer(self, sz=None, buf=None, policy=MemoryPolicy.OBSERVE):
		"""
		Use a variant of the copy-and-swap trick for assignment operators.
		This is sub-optimal in terms of performance for two reasons:
		1/ The constructing and swapping of the (small) values
		  in the temporary object takes some time, and is not necessary.
		2/ If policy == COPY, we allocate the new buffer before
		  freeing the old one, precluding the possibility of
		  reusing that memory.
		I doubt that either of these problems could be optimized away,
		but the second is probably no a common case, and the first is minor.
		I don't expect resetBuffer to be a common operation, so I'm willing to
		bite the performance bullet to make the method this simple.
		"""
		if buf is None:
			if sz is None:
				self.__rBase = 0
				self.__rBound = 0
				self.__wBase = 0
				# It isn't safe to write into a buffer we don't own.
				if self.__owner:
					self.__wBound = self.__wBase
					self.__bufferSize = 0
			else:
				# Construct the new buffer and move it into ourself
				self._swap(ISharedMemoryBuffer(sz))
		else:
			self._swap(ISharedMemoryBuffer(sz))

	def readEnd(self):
		"""
		return number of bytes read
		"""
		bytes_ = self.__rBase
		if self.__rBase == self.__wBase:
			self.resetBuffer()
		return bytes_

	def writeEnd(self):
		"""
		Return number of bytes written
		"""
		return self.__wBase

	def availableRead(self):
		return self.__wBase - self.__rBase

	def availableWrite(self):
		return self.wBound - self.wBase

	def getWritePtr(self, sz):
		self._ensureCanWrite(sz)
		return self.__wBase

	def wroteBytes(self, sz):
		avail = self.availableWrite()
		if sz > avail:
			raise TTransportException(message="Client wrote more bytes than size of buffer.")
		self.__wBase += sz

	def getBufferSize(self):
		return self.__bufferSize

	def getMaxBufferSize(self):
		return self.__maxBufferSize

	def setMaxBufferSize(self, maxSize):
		if maxSize < self.__bufferSize:
			raise TTransportException(message="Maximum buffer size would be less than current buffer size")

	def setBufferSize(self, new_size):
		self.__buffer.resize(new_size)
		self.__rBase = min(self.__rBase, new_size)
		self.__rBound = min(self.__rBound, new_size)
		self.__wBase = min(self.__wBase, new_size)
		self.__wBound = new_size;
		self.__bufferSize = new_size;

	def read(self, sz):
		"""
		Fast-path read.

		When we have enough data buffered to fulfill the read, we can satisfy it
		with a single memcpy, then adjust our internal pointers.  If the buffer
		is empty, we call out to our slow path.
		"""
		new_rBase = self.__rBase + sz
		if new_rBase <= self.__rBound:
			self.__rBase = new_rBase
			return self.__buffer[new_rBase - sz:new_rBase]
		return self._readSlow(sz)

	def write(self, buf):
		"""
		Fast-path write.

		When we have enough empty space in our buffer to accommodate the write, we
		can satisfy it with a single copy, then adjust our internal pointers.
		If the buffer is full, we call out to our slow path.
		"""
		new_wBase = self.__wBase + len(buf)
		if new_wBase <= self.__wBound:
			self.__wBase = new_wBase
			self.__buffer.write(buf)
			return
		self._writeSlow(buf)

	def consume(self, sz):
		"""
		Consume doesn't require a slow path.
		"""
		if sz <= self.__rBound - self.__rBase:
			self.__rBase_ += len
		else:
			raise TTransportException(message="consumed more than available")

	def flush(self):
		pass

	def setReadBuffer(self, buf, sz):
		self.__rBase = buf
		self.__rBound = buf + sz

	def setWriteBuffer(self, buf, sz):
		self.__wBase = buf
		self.__Bound = buf + sz

	def _swap(self, that):
		self.__buffer, that.__buffer = that.__buffer, self.__buffer
		self.__bufferSize, that.__bufferSize = that.__bufferSize, self.__bufferSize
		self.__rBase, that.__rBase = that.__rBase, self.__rBase
		self.__rBound, that.__rBound = that.__rBound, self.__rBound
		self.__wBase, that.__wBase = that.__wBase, self.__wBase
		self.__wBound, that.__wBound = that.__wBound, self.__wBound

		self.__owner, that.__owner = that.__owner, self.__owner

	def _ensureCanWrite(self, sz):
		# Check available space
		avail = self.availableWrite()
		if sz <= avail:
			return

		if not self.__owner:
			raise TTransportException(message="Insufficient space in external MemoryBuffer")

		# Grow the buffer as necessary.
		new_size = self.__bufferSize
		while sz > avail:
			if new_size > self.__maxBufferSize / 2:
				if self.availableWrite() + self.__maxBufferSize - self.__bufferSize < sz:
					raise TTransportException(message="Internal buffer size overflow")
				new_size = self.__maxBufferSize
			new_size = new_size * 2 if new_size > 0 else 1
			avail = self.availableWrite() + (new_size - self.__bufferSize)
		self.setBufferSize(new_size)

	def _computeRead(self, sz):
		# Correct rBound_ so we can use the fast path in the future.
		self.__rBound = self.__wBase

		# Decide how much to give.
		give = min(sz, self.availableRead())

		# Preincrement rBase_ so the caller doesn't have to
		self.__rBase += give

		return (self.__rBase - give, give)

	def _readSlow(self, sz):
		start, give = self._computeRead(sz)
		return self.__buffer[start: start + give]

	def _writeSlow(self, buf):
		self.ensureCanWrite(len(buf))
		self.__buffer.write(buf)
		self.__wBase += len(buf)

	def __initCommon(self, buf, size, owner, wPos):
		self.__maxBufferSize = sys.maxsize

		if buf is None and size != 0:
			assert owner
			buf = mmap.mmap(fileno=-1, length=size, access=mmap.ACCESS_WRITE)

		self.__buffer = buf
		self.__bufferSize = size

		self.__rBase = 0
		self.__rBound = wPos

		self.__wBase = wPos
		self.__wBound = self.__bufferSize

		self.__owner = owner
