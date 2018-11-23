import builtins
import collections


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
