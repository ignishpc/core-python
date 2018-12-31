from .IRawIterator import IRawReadIterator, IRawWriteIterator, readToWrite


class IRawIndexMemoryWriteIterator(IRawWriteIterator):

	def __init__(self, men):
		super().__init__(men)

	def write(self, obj):
		self._raw._index.append(self._raw._rawMemory.writeEnd())
		super().write(obj)


class IRawIndexMemoryReadIterator(IRawReadIterator):

	def __init__(self, men):
		super().__init__(men)

	def skip(self, n):
		self._elems += n
		self._raw._rawMemory.setReadBuffer(self._raw._index[self._elems])
