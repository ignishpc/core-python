import sys
import os
import importlib.util
import cloudpickle


class IObjectLoader:

	def __fileLoad(self, name):
		values = name.split(":")
		if len(values) != 2:
			raise NameError(name + " is not a valid python class")
		path = values[0]
		className = values[1]
		spec = importlib.util.spec_from_file_location(name=className,location=path)
		module = importlib.util.module_from_spec(spec)
		spec.loader.exec_module(module)
		classObject = getattr(module, className)
		return classObject()

	def __deserializate(self, binary):
		return cloudpickle.loads(binary)

	def load(self, source):
		if type(source) == str:
			return self.__fileLoad(source)
		else:
			return self.__deserializate(source)
