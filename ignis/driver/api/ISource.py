from types import FunctionType

import ignis.rpc.source.ttypes
from ignis.executor.core.ILibraryLoader import ILibraryLoader
from ignis.executor.core.protocol.IObjectProtocol import IObjectProtocol
from ignis.executor.core.transport.IMemoryBuffer import IMemoryBuffer


class _IFunctionLambda:

	def __init__(self, f):
		self.call = f

	def before(self, context):
		pass

	def after(self, context):
		pass


class ISource:

	def __init__(self, src, native=False):
		self.__native = native
		self.__inner = ignis.rpc.source.ttypes.ISource()
		obj = ignis.rpc.source.ttypes.IEncoded()
		if isinstance(src, str):
			obj.name = self.__src
		elif isinstance(src, FunctionType):
			obj.bytes = ILibraryLoader.pickle(_IFunctionLambda(src))
		else:
			obj.bytes = ILibraryLoader.pickle(src)
		self.__inner.obj = obj

	@classmethod
	def wrap(cls, src):
		if isinstance(src, ISource):
			return src
		else:
			return ISource(src)

	def addParam(self, name, value):
		buffer = IMemoryBuffer(1000)
		proto = IObjectProtocol(buffer)
		proto.writeObject(value, self.__native)
		self.__inner.params[name] = buffer.getBufferAsBytes()
		return self

	def rpc(self):
		return self.__inner
