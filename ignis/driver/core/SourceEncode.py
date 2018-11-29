from ignis.executor.api.function.IFunction import IFunction
from ignis.executor.api.function.IFlatFunction import IFlatFunction
from ignis.executor.api.function.IFunction2 import IFunction2
from ignis.rpc.source.ttypes import ISource
import cloudpickle
import types


def __dump(sc):
	return ISource(bytes=cloudpickle.dumps(sc))


def __encodeIFunction(sc):
	args = sc.__code__.co_argcount
	if args > 1:
		class Wrapper(IFunction):
			def call(self, elem, context):
				sc(elem, context)
	else:
		class Wrapper(IFunction):
			def call(self, elem, context):
				sc(elem)
	return __dump(Wrapper())


def __encodeIFlatFunction(sc):
	args = sc.__code__.co_argcount
	if args > 1:
		class Wrapper(IFlatFunction):
			def call(self, elem, context):
				sc(elem, context)
	else:
		class Wrapper(IFlatFunction):
			def call(self, elem, context):
				sc(elem)
	return __dump(Wrapper())


def __encodeIFunction2(sc):
	args = sc.__code__.co_argcount
	if args > 2:
		class Wrapper(IFunction2):
			def call(self, elem1, elem2, context):
				sc(elem1, elem2, context)
	else:
		class Wrapper(IFunction2):
			def call(self, elem1, elem2, context):
				sc(elem1, elem2)
	return __dump(Wrapper())


def encode(sc, iface):
	if isinstance(sc, str):
		return ISource(name=sc)
	if isinstance(sc, types.FunctionType):
		if iface == IFunction:
			return __encodeIFunction(sc)
		elif iface == IFlatFunction:
			return __encodeIFlatFunction(sc)
		elif iface == IFunction2:
			return __encodeIFunction2(sc)
		else:
			raise ValueError()
	return __dump(sc)
