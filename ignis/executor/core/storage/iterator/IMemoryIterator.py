from .ICoreIterator import ICoreReadIterator, ICoreWriteIterator, readToWrite


class IMemoryWriteIterator(ICoreReadIterator):

	def __init__(self, memory):
		self._memory = memory

	def write(self, obj):
		self._memory._elems += 1
		self._memory._data.append(obj)


class IMemoryReadIterator(ICoreReadIterator):

	def __init__(self, memory):
		self._memory = memory
		self._elems = 0

	def hasNext(self):
		return self._elems < self._memory._elems

	def next(self):
		self._elems += 1
		return self._memory._data[self._elems - 1]
