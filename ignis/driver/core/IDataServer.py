from ignis.executor.api.IManager import IManager
from ignis.executor.core.modules.IPostmanModule import IPostmanModule
from ignis.data.IObjectProtocol import IObjectProtocol
from ignis.data.IBytearrayTransport import IBytearrayTransport
from ignis.data.IZlibTransport import IZlibTransport


def parseBinary(binary, manager):
	result = list()
	trans = IBytearrayTransport(binary)
	while trans.available() > 0:
		ctrans = IZlibTransport(trans)
		proto = IObjectProtocol(ctrans)
		data = proto.readObject(manager)
		for elem in data:
			result.append(elem)
	return result


class IDataServer:

	def __init__(self, manager):
		self.__manager = manager

	def __enter__(self):
		return self

	def __exit__(self, exc_type, exc_val, exc_tb):
		pass

	def getResult(self):
		raise NotImplementedError()
