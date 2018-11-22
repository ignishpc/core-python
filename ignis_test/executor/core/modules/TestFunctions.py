from ignis.executor.api.function.IFunction import IFunction
from ignis.executor.api.function.IFlatFunction import IFlatFunction


class MapFunction(IFunction):

	def call(self, elem, context):
		return str(elem)


class FlatmapFunction(IFlatFunction):

	def call(self, elem, context):
		result = list()
		if elem % 2 == 0:
			result.append(str(elem))
		return result


class FilterFunction(IFunction):

	def call(self, elem, context):
		return elem % 2 == 0
