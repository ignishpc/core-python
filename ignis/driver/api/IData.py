from ignis.driver.api.Ignis import Ignis
from ignis.driver.api.IDriverException import IDriverException
import ignis.driver.core.SourceEncode as Se


class IData:

	def __init__(self, _id=None):
		self._id = _id

	def setName(self, name):
		try:
			with Ignis._pool.client() as client:
				self._id = client.getIDataService().setName(self._id, name)
		except Exception as ex:
			raise IDriverException(ex) from None

	def map(self, fun):
		try:
			with Ignis._pool.client() as client:
				return IData(client.getIDataService()._map(self._id, Se.encode(fun, Se.IFunction)))
		except Exception as ex:
			raise IDriverException(ex) from None

	def streamingMap(self, fun, ordered=True):
		try:
			with Ignis._pool.client() as client:
				return IData(client.getIDataService().streamingMap(self._id, Se.encode(fun, Se.IFunction), ordered))
		except Exception as ex:
			raise IDriverException(ex) from None

	def flatmap(self, fun):
		try:
			with Ignis._pool.client() as client:
				return IData(client.getIDataService().flatmap(self._id, Se.encode(fun, Se.IFlatFunction)))
		except Exception as ex:
			raise IDriverException(ex) from None

	def streamingFlatmap(self, fun, ordered=True):
		try:
			with Ignis._pool.client() as client:
				return IData(
					client.getIDataService().streamingFlatmap(self._id, Se.encode(fun, Se.IFlatFunction), ordered)
				)
		except Exception as ex:
			raise IDriverException(ex) from None

	def filter(self, fun):
		try:
			with Ignis._pool.client() as client:
				return IData(client.getIDataService().filter(self._id, Se.encode(fun, Se.IFunction)))
		except Exception as ex:
			raise IDriverException(ex) from None

	def streamingFilter(self, fun, ordered=True):
		try:
			with Ignis._pool.client() as client:
				return IData(client.getIDataService().streamingFilter(self._id, Se.encode(fun, Se.IFunction), ordered))
		except Exception as ex:
			raise IDriverException(ex) from None

	def reduceByKey(self, fun):
		try:
			with Ignis._pool.client() as client:
				return IData(client.getIDataService().reduceByKey(self._id, Se.encode(fun, Se.IFunction2)))
		except Exception as ex:
			raise IDriverException(ex) from None

	def shuffle(self):
		try:
			with Ignis._pool.client() as client:
				return IData(client.getIDataService().shuffle(self._id))
		except Exception as ex:
			raise IDriverException(ex) from None

	def saveAsTextFile(self, path, join):
		try:
			with Ignis._pool.client() as client:
				client.getIDataService().saveAsTextFile(self._id, path, join)
		except Exception as ex:
			raise IDriverException(ex) from None

	def saveAsJsonFile(self, path, join):
		try:
			with Ignis._pool.client() as client:
				client.getIDataService().saveAsJsonFile(self._id, path, join)
		except Exception as ex:
			raise IDriverException(ex) from None

