from ignis.driver.api.Ignis import Ignis
from ignis.driver.api.IDriverException import IDriverException
from ignis.driver.api.IJob import IJob


class ICluster:

	def __init__(self, properties):
		try:
			with Ignis._pool.client() as client:
				self._id = client.getIClusterService().newInstance(properties._id)
		except Exception as ex:
			raise IDriverException(ex) from ex

	def setName(self, name):
		try:
			with Ignis._pool.client() as client:
				client.getIClusterService().setName(self._id, name)
		except Exception as ex:
			raise IDriverException(ex) from ex

	def createJob(self, type, properties=None):
		return IJob(self, type, properties)

	def sendFiles(self, source, target):
		try:
			with Ignis._pool.client() as client:
				client.getIClusterService().sendFiles(self._id, source, target)
		except Exception as ex:
			raise IDriverException(ex) from ex

	def sendCompressedFile(self, source, target):
		try:
			with Ignis._pool.client() as client:
				client.getIClusterService().sendCompressedFile(self._id, source, target)
		except Exception as ex:
			raise IDriverException(ex) from ex
