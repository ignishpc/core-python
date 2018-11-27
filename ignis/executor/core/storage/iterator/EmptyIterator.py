from .ICoreIterator import ICoreReadIterator


class IEmptyReadIterator(ICoreReadIterator):

	def next(self):
		raise ValueError("Empty IObject")

	def hasNext(self):
		return False