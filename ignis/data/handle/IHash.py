import builtins
import collections


class IHash:

	def __init__(self, obj):
		self.obj = obj

	def __eq__(self, o):
		return isinstance(o, IHash) and self.obj == o.obj

	def __hash__(self) -> int:
		return self.hash(self.obj)

	@staticmethod
	def hash(obj):
		if isinstance(obj, collections.Hashable):
			return builtins.hash(obj)
		if isinstance(obj, collections.Iterable):
			if type(obj) == dict:
				it = obj.items()
			else:
				it = iter(obj)
			value = 0
			for elem in it:
				value += hash(elem)
		return builtins.hash(str(obj))
