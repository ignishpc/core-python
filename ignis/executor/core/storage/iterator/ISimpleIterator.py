from .ICoreIterator import ICoreWriteIterator, ICoreReadIterator


class IBasicReadIterator(ICoreReadIterator):

	def __init__(self, next, hasNext):
		self.__elems = 0
		self.next = next
		self.hasNext = hasNext

	def skip(self, n):
		while self.hasNext() and n > 0:
			self.next()
			n = n - 1


class IBasicWriteIterator(ICoreWriteIterator):

	def __init__(self, write):
		self.write = write
