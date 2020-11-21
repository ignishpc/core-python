import importlib.util

import cloudpickle


class ILibraryLoader:

	@classmethod
	def load(cls, name):
		values = name.split(":")
		if len(values) != 2:
			raise NameError(name + " is not a valid python class")
		path = values[0]
		className = values[1]
		spec = importlib.util.spec_from_file_location(name=className, location=path)
		module = importlib.util.module_from_spec(spec)
		spec.loader.exec_module(module)
		classObject = getattr(module, className)
		return classObject()

	@classmethod
	def unpickle(cls, bytes):
		return cloudpickle.loads(bytes)

	@classmethod
	def pickle(cls, src):
		return cloudpickle.dumps(src)
