from ignis.driver.api.Ignis import Ignis
from ignis.driver.api.IDriverException import IDriverException
from ignis.driver.api.IData import IData


class IJob:

	def __init__(self, cluster, type, properties=None):
		try:
			with Ignis._pool.client() as client:
				if properties:
					self._id = client.getIJobService().newInstance3(cluster._id, type, properties._id)
				else:
					self._id = client.getIJobService().newInstance(cluster._id, type)
		except Exception as ex:
			raise IDriverException(ex)

	def setName(self, name):
		try:
			with Ignis._pool.client() as client:
				client.getIJobService().setName(self._id, name)
		except Exception as ex:
			raise IDriverException(ex)

	def importData(self, data):
		try:
			with Ignis._pool.client() as client:
				return IData(client.getIJobService().importData(self._id, data))
		except Exception as ex:
			raise IDriverException(ex)

	def readFile(self, path):
		try:
			with Ignis._pool.client() as client:
				return IData(client.getIJobService().readFile(self._id, path))
		except Exception as ex:
			raise IDriverException(ex)
