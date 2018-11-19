import re


class IPropertyParser:

	def __init__(self, properties):
		self.__properties = properties
		self.__bool = re.compile("y|Y|yes|Yes|YES|true|True|TRUE|on|On|ON")

	def getString(self, key):
		return self.__properties[key]

	def getInt(self, key):
		value = self.getString(key)
		try:
			return int(value)
		except ValueError as ex:
			raise ValueError(key + " must be a number, find '" + value + "'")

	def getBool(self, key):
		return self.__bool.match(self.getString(key))
