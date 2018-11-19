from .ICoreIterator import ICoreWriteIterator, ICoreReadIterator


class ISimpleReadIterator(ICoreReadIterator):

	def __init__(self, next, hasNext, skip = None):
		self._elems = 0
		self.next = next.__get__(self)
		self.hasNext = hasNext.__get__(self)
		if skip:
			self.skip = skip.__get__(self)

	def skip(self, n):
		while self.hasNext() and n > 0:
			self.next()
			n = n - 1


class ISimpleWriteIterator(ICoreWriteIterator):

	def __init__(self, write):
		self.write = write.__get__(self)
