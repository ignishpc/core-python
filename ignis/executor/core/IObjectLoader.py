import sys
import os
import importlib.util
import cloudpickle

class IObjectLoader:

	def __fileLoad(self, name):
		split = name.split(":")
		if len(split) != 2:
			raise NameError(name + " is not a valid python class")
		path = split[0]
		searchPath = os.path.dirname(path)
		className = split[1]

		sys.path.insert(0, searchPath)
		try:
			spec =importlib.util.spec_from_file_location(className, path)
			module = importlib.util.module_from_spec(spec)
			classReference = module.__dict__[className]
		finally:
			sys.path.remove(searchPath)
		object = classReference()
		return object

	def __deserializate(self, binary):
		return cloudpickle.loads(binary)

	def load(self, source):
		if type(source) == str:
			return self.__fileLoad(source)
		else:
			return self.__deserializate(source)