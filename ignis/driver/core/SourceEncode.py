from ignis.executor.api.function.IFunction import IFunction
from ignis.executor.api.function.IFlatFunction import IFlatFunction
from ignis.executor.api.function.IFunction2 import IFunction2
from ignis.rpc.source.ttypes import ISource, IEncoded
from ignis.data.IObjectProtocol import IObjectProtocol
from ignis.data.IBytearrayTransport import IBytearrayTransport
import cloudpickle
import types


def __dump(sc):
	return IEncoded(bytes=cloudpickle.dumps(sc))


def __encodeIFunction(sc):
	args = sc.__code__.co_argcount
	if args > 1:
		class Wrapper(IFunction):
			def call(self, elem, context):
				return sc(elem, context)
	else:
		class Wrapper(IFunction):
			def call(self, elem, context):
				return sc(elem)
	return __dump(Wrapper())


def __encodeIFlatFunction(sc):
	args = sc.__code__.co_argcount
	if args > 1:
		class Wrapper(IFlatFunction):
			def call(self, elem, context):
				return sc(elem, context)
	else:
		class Wrapper(IFlatFunction):
			def call(self, elem, context):
				return sc(elem)
	return __dump(Wrapper())


def __encodeIFunction2(sc):
	args = sc.__code__.co_argcount
	if args > 2:
		class Wrapper(IFunction2):
			def call(self, elem1, elem2, context):
				return sc(elem1, elem2, context)
	else:
		class Wrapper(IFunction2):
			def call(self, elem1, elem2, context):
				return sc(elem1, elem2)
	return __dump(Wrapper())


def __encodeArgs(args, manager):
	result = dict()
	for name, arg in args.items():
		result[name] = bytearray()
		trans = IBytearrayTransport(result[name])
		proto = IObjectProtocol(trans)
		proto.writeObject(arg, manager, native=False)
	return result


def encode(sc, iface):
	from ignis.driver.api.IData import IData
	if isinstance(sc, str):
		return ISource(obj=IEncoded(name=sc))
	if isinstance(sc, IData.WithArgs):
		return ISource(obj=IEncoded(name=sc._func), params=__encodeArgs(sc._args, sc._manager))
	if isinstance(sc, types.FunctionType):
		if iface == IFunction:
			return ISource(obj=__encodeIFunction(sc))
		elif iface == IFlatFunction:
			return ISource(obj=__encodeIFlatFunction(sc))
		elif iface == IFunction2:
			return ISource(obj=__encodeIFunction2(sc))
		else:
			raise ValueError()
	return ISource(obj=__dump(sc))
