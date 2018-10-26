from ignis.driver.api.Ignis import Ignis
from ignis.driver.api.IDriverException import IDriverException
from ignis.rpc.source.ttypes import ISource


class IData:

	def __init__(self, _id=None):
		self._id = _id

	def setName(self, name):
		try:
			with Ignis._pool.client() as client:
				self._id = client.getIDataService().setName(self._id, name)
		except Exception as ex:
			raise IDriverException(ex) from ex

	def map(self, fun):
		try:
			with Ignis._pool.client() as client:
				return IData(client.getIDataService()._map(self._id, self.__enconde(fun)))
		except Exception as ex:
			raise IDriverException(ex) from ex

	def streamingMap(self, fun, ordered=True):
		try:
			with Ignis._pool.client() as client:
				return IData(client.getIDataService().streamingMap(self._id, self.__enconde(fun), ordered))
		except Exception as ex:
			raise IDriverException(ex) from ex

	def flatmap(self, fun):
		try:
			with Ignis._pool.client() as client:
				return IData(client.getIDataService().flatmap(self._id, self.__enconde(fun)))
		except Exception as ex:
			raise IDriverException(ex) from ex

	def streamingFlatmap(self, fun, ordered=True):
		try:
			with Ignis._pool.client() as client:
				return IData(client.getIDataService().streamingFlatmap(self._id, self.__enconde(fun), ordered))
		except Exception as ex:
			raise IDriverException(ex) from ex

	def filter(self, fun):
		try:
			with Ignis._pool.client() as client:
				return IData(client.getIDataService().filter(self._id, self.__enconde(fun)))
		except Exception as ex:
			raise IDriverException(ex) from ex

	def streamingFilter(self, fun, ordered=True):
		try:
			with Ignis._pool.client() as client:
				return IData(client.getIDataService().streamingFilter(self._id, self.__enconde(fun), ordered))
		except Exception as ex:
			raise IDriverException(ex) from ex

	def reduceByKey(self, fun):
		try:
			with Ignis._pool.client() as client:
				return IData(client.getIDataService().reduceByKey(self._id, self.__enconde(fun)))
		except Exception as ex:
			raise IDriverException(ex) from ex

	def shuffle(self):
		try:
			with Ignis._pool.client() as client:
				return IData(client.getIDataService().shuffle(self._id))
		except Exception as ex:
			raise IDriverException(ex) from ex

	def saveAsTextFile(self, path, join):
		try:
			with Ignis._pool.client() as client:
				client.getIDataService().saveAsTextFile(self._id, path, join)
		except Exception as ex:
			raise IDriverException(ex) from ex

	def saveAsJsonFile(self, path, join):
		try:
			with Ignis._pool.client() as client:
				client.getIDataService().saveAsJsonFile(self._id, path, join)
		except Exception as ex:
			raise IDriverException(ex) from ex

	def __enconde(self, fun):
		if isinstance(fun, str):
			return ISource(name=fun)
