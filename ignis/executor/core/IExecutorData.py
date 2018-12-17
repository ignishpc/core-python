from .IPostBox import IPostBox
from .IPropertyParser import IPropertyParser
from ..api.IContext import IContext

class IExecutorData:

	def __init__(self):
		self.__loadedObject = None
		self.__context = IContext()
		self.__postBox = IPostBox()
		self.__context = IContext()
		self.__parser = IPropertyParser(self.__context.getProperties())

	def loadObject(self, obj=None):
		aux = self.__loadedObject
		if obj:
			self.__loadedObject = obj
		return aux

	def deleteLoadObject(self):
		self.__loadedObject = None

	def getContext(self):
		return self.__context

	def getParser(self):
		return self.__parser

	def getPostBox(self):
		return self.__postBox

	def getWorkers(self):
		return self.__parser.getInt("ignis.executor.cores")

	def __getstate__(self):
		raise ValueError("This object can never be serialized")