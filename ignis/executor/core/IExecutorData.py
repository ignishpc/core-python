from ignis.executor.core.ILibraryLoader import ILibraryLoader
from ignis.executor.core.IMpi import IMpi
from ignis.executor.core.IPartitionTools import IPartitionTools
from ignis.executor.core.IPropertyParser import IPropertyParser
from ignis.executor.api.IContext import IContext
from ignis.rpc.source.ttypes import ISource, IEncoded
import logging
import os

logger = logging.getLogger(__name__)


class IExecutorData:

	def __init__(self):
		self.__context = IContext()
		self.__properties = IPropertyParser(self.__context.props())
		self.__library_loader = ILibraryLoader()
		self.__partition_tools = IPartitionTools(self.__properties, self.__context)
		self.__mpi = IMpi(self.__properties, self.__context)
		self.__partitions = None
		self.__variables = dict()

	def getPartitions(self):
		return self.__partitions

	def setPartitions(self, group):
		old = self.__partitions
		self.__partitions = group
		return old

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
		info = self.__properties.jobDirectory() + "/info"
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

		with open(self.infoDirectory() + "/sources" + str(self.__context.executorId()) + ".bak", "a") as backup:
			backup.write(source.obj.name + "\n")
		return lib

	def reloadLibraries(self):
		backup_path = self.infoDirectory() + "/sources" + str(self.__context.executorId()) + ".bak", "a"
		if os.path.isfile(backup_path):
			logger.info("Function backup found, loading")
			with open(backup_path, "r") as backup:
				loaded = set()
				source = ISource(obj=IEncoded())
				for line in backup:
					if line not in loaded:
						try:
							source.obj.name = line
							self.loadLibrary(source)
							loaded.add(line)
						except Exception as ex:
							logger.error(str(ex))

	def getContext(self):
		return self.__context

	def getProperties(self):
		return self.__properties

	def getPartitionTools(self):
		return self.__partition_tools

	def mpi(self):
		return self.__mpi
