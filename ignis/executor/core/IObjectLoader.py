import importlib.util
import cloudpickle
from ignis.data.IObjectProtocol import IObjectProtocol
from ignis.data.IBytearrayTransport import IBytearrayTransport

class IObjectLoader:

	def load(self, name):
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

	def decode(self, binary):
		return cloudpickle.loads(binary)

	def decodeParams(self, params, context):
		manager = context.getManager()
		result = dict()
		for name, arg in params.items():
			trans = IBytearrayTransport(arg)
			proto = IObjectProtocol(trans)
			result[name] = proto.readObject(manager)
		context.getVariables().update(result)
