import logging
import traceback
from ignis.rpc.exception.ttypes import IRemoteException
from ..storage.IMemoryObject import IMemoryObject
from ..storage.IRawIndexMemoryObject import IRawIndexMemoryObject
from ..storage.IRawMemoryObject import IRawMemoryObject
from ..IObjectLoader import IObjectLoader
from ..IPropertyParser import IPropertyParser

logger = logging.getLogger(__name__)


class IModule:

	def __init__(self, executorData):
		self._executorData = executorData

	def raiseRemote(self, ex):
		raise IRemoteException(message=str(ex), stack=traceback.format_exc())

	@staticmethod
	def getIObjectStatic(context, elems=1000, bytes=None, storage=None, shared=None):
		parser = IPropertyParser(context.getProperties())
		if bytes is None:
			bytes = elems * 128
		if storage is None:
			storage = parser.getString("ignis.executor.storage")
		if shared is None:
			shared = parser.getInt("ignis.executor.cores") > 1
		manager = context.getManager()
		serialization = parser.getString("ignis.transport.serialization")
		native = serialization == 'native'
		if storage == IMemoryObject.TYPE and not shared:
			return IMemoryObject(manager, native)
		if storage == IRawMemoryObject.TYPE:
			compression = parser.getInt("ignis.executor.storage.compression")
			return IRawMemoryObject(compression, manager, native, bytes, shared)
		return IRawIndexMemoryObject(manager, native, elems, bytes, shared)

	def getIObject(self, *args, **kwargs):
		return self.getIObjectStatic(self._executorData.getContext(), *args, **kwargs)

	@staticmethod
	def loadSource(source, context):
		logging.info("IModule loading function")
		loader = IObjectLoader()
		if source.obj.bytes:
			result = loader.decode(source.obj.bytes)
		else:
			result = loader.load(source.obj.name)
			if source.params:
				logger.info("IModule loading arguments")
				loader.decodeParams(source.params, context)
		logging.info("IModule function loaded")
		return result
