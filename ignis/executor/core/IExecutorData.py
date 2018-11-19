from .IPostBox import IPostBox
from .IPropertyParser import IPropertyParser
from ..api.IContext import IContext

class IExecutorData:

	def __init__(self):
		self.__context = IContext()
		self.__postBox = IPostBox()
		self.__context = IContext()

	def loadObject(self, obj=None):
		if obj:
			self.__loadedObject = obj
		return self.__loadedObject

	def deleteLoadObject(self):
		self.__loadedObject = None

	def getContext(self):
		return self.__context

	def getParser(self):
		return self.__parser

	def getPostBox(self):
		return self.__postBox

	def getThreads(self):
		return self.__threads