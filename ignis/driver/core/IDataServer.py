from ignis.executor.api.IManager import IManager
from ignis.executor.core.modules.IPostmanModule import IPostmanModule
from ignis.data.IObjectProtocol import IObjectProtocol
from ignis.data.IBytearrayTransport import IBytearrayTransport


def parseBinary(binary, manager):
	trans = IBytearrayTransport(binary)
	proto = IObjectProtocol(trans)
	return proto.readObject(manager)


class IDataServer:

	def __init__(self, manager):
		self.__manager = manager

	def __enter__(self):
		return self

	def __exit__(self, exc_type, exc_val, exc_tb):
		pass

	def getResult(self):
		raise NotImplementedError()
