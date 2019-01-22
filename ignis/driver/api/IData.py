from ignis.driver.api.Ignis import Ignis
from ignis.driver.api.IDriverException import IDriverException
import ignis.driver.core.SourceEncode as Se
import ignis.driver.core.IDataServer as IDataServer


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

	def keyBy(self, fun):
		try:
			with Ignis._pool.client() as client:
				return IData(client.getIDataService().keyBy(self._id, Se.encode(fun, Se.IFunction)))
		except Exception as ex:
			raise IDriverException(ex) from None

	def streamingKeyBy(self, fun, ordered=True):
		try:
			with Ignis._pool.client() as client:
				return IData(client.getIDataService().streamingKeyBy(self._id, Se.encode(fun, Se.IFunction), ordered))
		except Exception as ex:
			raise IDriverException(ex) from None

	def reduceByKey(self, fun):
		try:
			with Ignis._pool.client() as client:
				return IData(client.getIDataService().reduceByKey(self._id, Se.encode(fun, Se.IFunction2)))
		except Exception as ex:
			raise IDriverException(ex) from None

	def values(self):
		try:
			with Ignis._pool.client() as client:
				return IData(client.getIDataService().values(self._id))
		except Exception as ex:
			raise IDriverException(ex) from None

	def shuffle(self):
		try:
			with Ignis._pool.client() as client:
				return IData(client.getIDataService().shuffle(self._id))
		except Exception as ex:
			raise IDriverException(ex) from None

	def parallelize(self):
		raise NotImplementedError()

	def first(self, light=False, manager=IDataServer.IManager()):
		return self.take(1, light, manager)[0]

	def take(self, n, light=False, manager=IDataServer.IManager()):
		try:
			if light:
				with Ignis._pool.client() as client:
					binary = client.getIDataService().take(self._id, n, True)
					return IDataServer.parseBinaryParts(binary, manager)
			else:
				with IDataServer.IDataServer(manager) as ds:
					with Ignis._pool.client() as client:
						client.getIDataService().take(self._id, n, False)
						return ds.getResult()
		except Exception as ex:
			raise IDriverException(ex) from None

	def takeSample(self, n, withRemplacement=False, seed=None, light=False, manager=IDataServer.IManager()):
		try:
			if seed is None:
				import time
				import random
				seed = random.Random(time.time()).randrange(2**31-1)
			if light:
				with Ignis._pool.client() as client:
					binary = client.getIDataService().takeSample(self._id, n, withRemplacement, seed, True)
					return IDataServer.parseBinaryParts(binary, manager)
			else:
				with IDataServer.IDataServer(manager) as ds:
					with Ignis._pool.client() as client:
						client.getIDataService().takeSample(self._id, n, withRemplacement, seed, False)
						return ds.getResult()
		except Exception as ex:
			raise IDriverException(ex) from None

	def collect(self, light=False, manager=IDataServer.IManager()):
		try:
			if light:
				with Ignis._pool.client() as client:
					binary = client.getIDataService().collect(self._id, True)
					return IDataServer.parseBinaryParts(binary, manager)
			else:
				with IDataServer.IDataServer(manager) as ds:
					with Ignis._pool.client() as client:
						client.getIDataService().collect(self._id, False)
						return ds.getResult()
		except Exception as ex:
			raise IDriverException(ex) from None

	def sort(self):
		try:
			with Ignis._pool.client() as client:
				return IData(client.getIDataService().sort(self._id))
		except Exception as ex:
			raise IDriverException(ex) from None

	def sortBy(self, fun):
		try:
			with Ignis._pool.client() as client:
				return IData(client.getIDataService().sortBy(self._id, Se.encode(fun, Se.IFunction)))
		except Exception as ex:
			raise IDriverException(ex) from None

	def collectAsMap(self, light=False, manager=IDataServer.IManager()):
		result = dict()
		data = self.collect(light, manager)
		size = len(data)
		for i in range(0, size):
			pair = data.pop()
			result[pair[0]] = pair[1]
		return result

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

	def cache(self):
		try:
			with Ignis._pool.client() as client:
				return client.getIDataService().cache(self._id)
			return self
		except Exception as ex:
			raise IDriverException(ex) from None

	def uncache(self):
		try:
			with Ignis._pool.client() as client:
				return client.getIDataService().uncache(self._id)
			return self
		except Exception as ex:
			raise IDriverException(ex) from None
