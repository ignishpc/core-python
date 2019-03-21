import importlib.util
import cloudpickle
from ignis.data.IObjectProtocol import IObjectProtocol
from ignis.data.IBytearrayTransport import IBytearrayTransport

class IObjectLoader:

	def __fileLoad(self, name):
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

	def __deserializate(self, binary):
		return cloudpickle.loads(binary)

	def __decodeArgs(self, source, context):
		manager = context.getManager()
		result = dict()
		for name, arg in source._args.items():
			trans = IBytearrayTransport(arg)
			proto = IObjectProtocol(trans)
			result[name] = proto.readObject(manager)
		context.getVariables().update(result)

	def load(self, source, context):
		if type(source) == str:
			obj = self.__fileLoad(source)
			if source._args:
				self.__decodeArgs(source, context)
			return obj
		else:
			return self.__deserializate(source)
