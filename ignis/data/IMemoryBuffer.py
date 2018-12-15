from thrift.transport.TTransport import TTransportBase, TTransportException
from enum import Enum
import sys
import mmap
import cffi
import os
import uuid


class IMemory:

	def __init__(self, size):
		self.__array = bytearray(size)

	def __getitem__(self, i):
		return self.__array[i]

	def __setitem__(self, i, value):
		self.__array[i] = value

	def resize(self, size):
		if len(self.__array) > size:
			self.__array = self.__array[0:size]

	def __len__(self):
		return len(self.__array)


class ISharedMemory:
	__ffi = cffi.FFI()
	__ffi.cdef("int shm_open(const char *name, int oflag, int mode);")
	__shm = __ffi.verify("#include <sys/mman.h>", libraries=["rt"])

	def __initMemory(self, name, size, create):
		self.__name = name
		self.__size = size
		char_name = bytes(self.__name, 'ascii')
		fd = self.__shm.shm_open(char_name, os.O_RDWR | os.O_CREAT | (os.O_EXCL if create else 0), 0o600)
		if fd < 0:
			errno = self.__ffi.errno
			raise OSError(errno, os.strerror(errno))
		os.ftruncate(fd, size)
		self.__mmap = mmap.mmap(fd, self.__size, access=mmap.ACCESS_WRITE)
		os.close(fd)

	def __init__(self, size):
		name = '/' + str(uuid.uuid4())
		self.__initMemory(name, size, True)

	def __getitem__(self, i):
		return self.__mmap[i]

	def __setitem__(self, i, value):
		self.__mmap[i] = value

	def resize(self, sz):
		self.__mmap.resize(sz)

	def __setstate__(self, st):
		name, size = st
		self.__initMemory(name, size, False)

	def __getstate__(self):
		return self.__name, self.__size

	def __len__(self):
		return self.__size


class IMemoryBuffer(TTransportBase):
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

	def __init__(self, sz=None, buf=None, policy=MemoryPolicy.OBSERVE, shared=False):
		"""
		Construct a ISharedMemoryBuffer

		@param buf    The initial contents of the buffer.
				   ISharedMemoryBuffer will not write to it if policy == OBSERVE.
		@param sz     The size of @c buf.
		@param policy See @link MemoryPolicy @endlink .
		"""
		if buf is None:
			if sz is None:
				self.__initCommon(None, IMemoryBuffer.defaultSize, True, 0, shared)
			else:
				self.__initCommon(None, sz, True, 0, shared)
		else:
			if policy == IMemoryBuffer.MemoryPolicy.OBSERVE or IMemoryBuffer.MemoryPolicy.TAKE_OWNERSHIP:
				self.__initCommon(buf, sz, policy == IMemoryBuffer.MemoryPolicy.TAKE_OWNERSHIP, sz, shared)
			elif policy == IMemoryBuffer.MemoryPolicy.COPY:
				self.__initCommon(None, sz, True, 0, shared)
				self.write(buf, sz)
			else:
				raise TTransportException(message="Invalid MemoryPolicy for ISharedMemoryBuffer")

	def isOpen(self):
		return True

	def peek(self):
		return self.__rBase_ < self.__wBase_

	def open(self):
		pass

	def close(self):
		pass

	def getBuffer(self):
		return self.__buffer

	def resetBuffer(self, sz=None, buf=None, policy=MemoryPolicy.OBSERVE):
		if buf is None:
			self.__rBase = 0
			self.__wBase = 0
			# It isn't safe to write into a buffer we don't own.
			if self.__owner:
				if sz is None:
					self.setBufferSize(IMemoryBuffer.defaultSize)
				else:
					self.setBufferSize(sz)
		else:
			self.__init__(sz, buf, policy)

	def readEnd(self):
		"""
		return number of bytes read
		"""
		return self.__rBase

	def writeEnd(self):
		"""
		Return number of bytes written
		"""
		return self.__wBase

	def availableRead(self):
		return self.__wBase - self.__rBase

	def availableWrite(self):
		return len(self) - self.__wBase

	def getBufferSize(self):
		return len(self)

	def __len__(self):
		return len(self.__buffer)

	def getMaxBufferSize(self):
		return self.__maxBufferSize

	def setMaxBufferSize(self, maxSize):
		if maxSize < len(self):
			raise TTransportException(message="Maximum buffer size would be less than current buffer size")

	def setBufferSize(self, new_size):
		self.__buffer.resize(new_size)
		self.__rBase = min(self.__rBase, new_size)
		self.__wBase = min(self.__wBase, new_size)

	def read(self, sz):
		# Decide how much to give.
		give = min(sz, self.availableRead())

		old_rBase = self.__rBase
		new_rBase = old_rBase + give
		self.__rBase = new_rBase
		return self.__buffer[old_rBase:new_rBase]

	def _computeRead(self, sz):
		# Decide how much to give.
		give = min(sz, self.availableRead())

		# Preincrement rBase_ so the caller doesn't have to
		self.__rBase += give

		return (self.__rBase - give, give)

	def _readSlow(self, sz):
		start, give = self._computeRead(sz)
		return self.__buffer[start: start + give]

	def write(self, buf):
		self._ensureCanWrite(len(buf))
		new_wBase = self.__wBase + len(buf)
		self.__buffer[self.__wBase:new_wBase] = buf
		self.__wBase = new_wBase

	def consume(self, sz):
		"""
		Consume doesn't require a slow path.
		"""
		if sz <= self.__wBase - self.__rBase:
			self.__rBase_ += len
		else:
			raise TTransportException(message="consumed more than available")

	def flush(self):
		pass

	def setReadBuffer(self, pos):
		self.__rBase = pos

	def setWriteBuffer(self, pos):
		self.__wBase = pos

	def _ensureCanWrite(self, sz):
		# Check available space
		avail = self.availableWrite()
		if sz <= avail:
			return

		if not self.__owner:
			raise TTransportException(message="Insufficient space in external MemoryBuffer")

		# Grow the buffer as necessary.
		new_size = len(self)
		while sz > avail:
			if new_size > self.__maxBufferSize / 2:
				if self.availableWrite() + self.__maxBufferSize - len(self) < sz:
					raise TTransportException(message="Internal buffer size overflow")
				new_size = self.__maxBufferSize
			new_size = new_size * 2 if new_size > 0 else 1
			avail = self.availableWrite() + (new_size - len(self))
		self.setBufferSize(new_size)

	def __initCommon(self, buf, size, owner, wPos, shared):
		self.__maxBufferSize = sys.maxsize

		if buf is None and size != 0:
			assert owner
			if shared:
				buf = ISharedMemory(size)
			else:
				buf = IMemory(size)
		assert buf
		self.__buffer = buf

		self.__rBase = 0
		self.__wBase = wPos
		self.__owner = owner

	def __getitem__(self, i):
		return self.__buffer[i]
