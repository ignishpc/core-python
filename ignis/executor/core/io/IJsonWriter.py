import json


class IJsonWriter(json.JSONEncoder):
	__types = dict()

	@classmethod
	def __setitem__(cls, key, value):
		cls.__types[key] = value

	@classmethod
	def __getitem__(cls, key):
		return cls.__types[key]

	@classmethod
	def __delitem__(cls, key):
		del cls.__types[key]

	def __init__(self, pretty=False):
		json.JSONEncoder.__init__(indent=4 if pretty else None)
		self.__types = IJsonWriter.__types.copy()

	def default(self, o):
		writer = self.__types.get(type(o), None)
		if writer:
			return writer(self, o)
		else:
			return o.__dict__
