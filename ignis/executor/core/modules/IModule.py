import logging
import traceback
from ignis.rpc.exception.ttypes import IRemoteException
from ..storage.IMemoryObject import IMemoryObject
from ..storage.IRawMemoryObject import IRawMemoryObject
from ..IObjectLoader import IObjectLoader

logger = logging.getLogger(__name__)


class IModule:

	def __init__(self, executorData):
		self._executorData = executorData

	def raiseRemote(self, ex):
		raise IRemoteException(message=str(ex), stack=traceback.format_exc())

	def getIObject(self, elems=1000, bytes=50 * 1024 * 1024, storage=None):
		if storage is None:
			storage = self._executorData.getParser().getString("ignis.executor.storage")
		manager = self._executorData.getContext().getManager()
		serialization = self._executorData.getParser().getString("ignis.transport.serialization")
		native = serialization == 'native'
		if storage == IRawMemoryObject.TYPE:
			compression = self._executorData.getParser().getInt("ignis.executor.storage.compression")
			return IRawMemoryObject(compression, manager, native, bytes)
		else:
			return IMemoryObject(manager, native, elems, bytes)

	@staticmethod
	def loadSource(source):
		logging.info("IModule loading function")
		loader = IObjectLoader()
		if source.name:
			result = loader.load(source.name)
		else:
			result = loader.load(source.bytes)
		logging.info("IModule function loaded")
		return result

	def memoryObject(self, obj=None):
		if obj is None:
			return self.getIObject(storage="memory")
		if type(obj) != IMemoryObject:
			menObj = self.getIObject(obj.getSize(), storage="memory")
			obj.moveTo(menObj)
			obj = menObj
		return obj
