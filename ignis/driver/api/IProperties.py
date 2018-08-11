from ignis.driver.api.Ignis import Ignis
from ignis.driver.api.IDriverException import IDriverException


class IProperties:

	def __init__(self, properties=None):
		try:
			with Ignis._pool.client() as client:
				if properties:
					self._id = client.getIPropertiesService().newInstance2(properties._id)
				else:
					self._id = client.getIPropertiesService().newInstance()
		except Exception as ex:
			raise IDriverException(ex) from ex

	def setProperty(self, key, value):
		try:
			with Ignis._pool.client() as client:
				client.getIPropertiesService().setProperty(self._id, key, value)
		except Exception as ex:
			raise IDriverException(ex) from ex

	def getProperty(self, key):
		try:
			with Ignis._pool.client() as client:
				client.getIPropertiesService().getProperty(self._id, key)
		except Exception as ex:
			raise IDriverException(ex) from ex

	def isProperty(self, key):
		try:
			with Ignis._pool.client() as client:
				client.getIPropertiesService().isProperty(self._id, key)
		except Exception as ex:
			raise IDriverException(ex) from ex

	def toDict(self):
		try:
			with Ignis._pool.client() as client:
				client.getIPropertiesService().toDict(self._id)
		except Exception as ex:
			raise IDriverException(ex) from ex

	def fromDict(self, _dict):
		try:
			with Ignis._pool.client() as client:
				client.getIPropertiesService().fromDict(self._id, _dict)
		except Exception as ex:
			raise IDriverException(ex) from ex

	def toFile(self, path):
		try:
			with Ignis._pool.client() as client:
				client.getIPropertiesService().toFile(self._id, path)
		except Exception as ex:
			raise IDriverException(ex) from ex

	def fromFile(self, path):
		try:
			with Ignis._pool.client() as client:
				client.getIPropertiesService().fromFile(self._id, path)
		except Exception as ex:
			raise IDriverException(ex) from ex

	def reset(self):
		try:
			with Ignis._pool.client() as client:
				client.getIPropertiesService().reset(self._id)
		except Exception as ex:
			raise IDriverException(ex) from ex
