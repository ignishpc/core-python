import ignis.rpc.source.ttypes


class ISource:

	def __init__(self, src):
		self.__src = src
		self.__inner = ignis.rpc.source.ttypes.ISource()

	@classmethod
	def wrap(ISource, src):
		if isinstance(src, ISource):
			return src
		else:
			return ISource(src)

	def addParam(self, name, value):
		# TODO
		return self

	def rpc(self):
		if self.__inner.obj:
			return self.__inner

		obj = ignis.rpc.source.ttypes.IEncoded()
		if isinstance(self.__src, str):
			obj.name = self.__src
		else:
			# TODO
			pass
		self.__inner.obj = obj

		return self.__inner
