import logging

from ignis.executor.api.IContext import IContext
from ignis.executor.core.ILibraryLoader import ILibraryLoader
from ignis.executor.core.IMpi import IMpi
from ignis.executor.core.IPartitionTools import IPartitionTools
from ignis.executor.core.IPropertyParser import IPropertyParser

logger = logging.getLogger(__name__)


class IExecutorData:

	def __init__(self):
		self.__context = IContext()
		self.__properties = IPropertyParser(self.__context.props())
		self.__library_loader = ILibraryLoader()
		self.__partition_tools = IPartitionTools(self.__properties, self.__context)
		self.__mpi = IMpi(self.__properties, self.__partition_tools, self.__context)
		self.__partitions = None
		self.__variables = dict()

	def getPartitions(self):
		return self.__partitions

	def setPartitions(self, group):
		old = self.__partitions
		self.__partitions = group
		return old

	def hasPartitions(self):
		return self.__partitions is not None

	def deletePartitions(self):
		self.__partitions = None

	def setVariable(self, key, value):
		self.__variables[key] = value

	def getVariable(self, key):
		return self.__variables[key]

	def removeVariable(self, key):
		del self.__variables[key]

	def clearVariables(self):
		self.__variables.clear()

	def infoDirectory(self):
		info = self.__properties.executorDirectory() + "/info"
		self.__partition_tools.createDirectoryIfNotExists(info)
		return info

	def loadLibrary(self, source):
		logger.info("Loading function")
		if source.obj.bytes is not None:
			lib = self.__library_loader.unpickle(source.obj.bytes)
		else:
			lib = self.__library_loader.load(source.obj.name)

		if source.params:
			logger.info("Loading user variables")
			for key, value in source.params.items():
				self.__context.vars[key] = self.__library_loader.unpickle(value)
		logger.info("Function loaded")

		return lib

	def getContext(self):
		return self.__context

	def getProperties(self):
		return self.__properties

	def getPartitionTools(self):
		return self.__partition_tools

	def mpi(self):
		return self.__mpi
