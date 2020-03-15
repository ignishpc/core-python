class IReadIterator:

	def next(self):
		raise NotImplementedError("not implemented")

	def hasNext(self):
		raise NotImplementedError("not implemented")

	def __iter__(self):
		return self

	def __next__(self):
		if self.hasNext():
			return self.next()
