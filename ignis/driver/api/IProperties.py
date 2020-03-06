from ignis.driver.api.Ignis import Ignis
from ignis.driver.api.IDriverException import IDriverException
import ignis.rpc.driver.exception.ttypes


class IProperties:

	def __init__(self, properties=None):
		try:
			with Ignis._pool.getClient() as client:
				if properties:
					self._id = client.getIPropertiesService().newInstance2(properties._id)
				else:
					self._id = client.getIPropertiesService().newInstance()
		except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
			raise IDriverException(ex.message, ex._cause)

	def setProperty(self, key, value):
		try:
			with Ignis._pool.getClient() as client:
				client.getIPropertiesService().setProperty(self._id, key, value)
		except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
			raise IDriverException(ex.message, ex._cause)

	def getProperty(self, key):
		try:
			with Ignis._pool.getClient() as client:
				return client.getIPropertiesService().getProperty(self._id, key)
		except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
			raise IDriverException(ex.message, ex._cause)

	def rmProperty(self, key):
		try:
			with Ignis._pool.getClient() as client:
				return client.getIPropertiesService().rmProperty(self._id, key)
		except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
			raise IDriverException(ex.message, ex._cause)

	def contains(self, key):
		try:
			with Ignis._pool.getClient() as client:
				return client.getIPropertiesService().contains(self._id, key)
		except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
			raise IDriverException(ex.message, ex._cause)

	def __getitem__(self, key):
		return self.getProperty(key)

	def __setitem__(self, key, value):
		self.setProperty(key, value)

	def __delitem__(self, key):
		self.rmProperty(key)

	def __contains__(self, key):
		return self.contains(key)

	def toMap(self, defaults=False):
		try:
			with Ignis._pool.getClient() as client:
				return client.getIPropertiesService().toMap(self._id, defaults)
		except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
			raise IDriverException(ex.message, ex._cause)

	def fromMap(self, _map):
		try:
			with Ignis._pool.getClient() as client:
				return client.getIPropertiesService().fromDict(self._id, _map)
		except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
			raise IDriverException(ex.message, ex._cause)

	def load(self, path):
		try:
			with Ignis._pool.getClient() as client:
				client.getIPropertiesService().load(self._id, path)
		except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
			raise IDriverException(ex.message, ex._cause)

	def store(self, path):
		try:
			with Ignis._pool.getClient() as client:
				client.getIPropertiesService().store(self._id, path)
		except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
			raise IDriverException(ex.message, ex._cause)

	def clear(self):
		try:
			with Ignis._pool.getClient() as client:
				client.getIPropertiesService().clear(self._id)
		except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
			raise IDriverException(ex.message, ex._cause)
