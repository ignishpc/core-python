from .IManager import IManager

class IContext:

	def __init__(self):
		self.__properties = dict()
		self.__manager = IManager()

	def __getitem__(self, key):
		return self.__properties[key]

	def __setitem__(self, key, value):
		self.__properties[key] = value

	def __delitem__(self, key):
		del self.__properties[key]

	def __contains__(self, key):
		return key in self.__properties

	def getProperties(self):
		return self.__properties

	def getManager(self):
		return self.__manager
