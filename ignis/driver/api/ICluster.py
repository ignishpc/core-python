from ignis.driver.api.Ignis import Ignis
from ignis.driver.api.IDriverException import IDriverException
import ignis.rpc.driver.exception.ttypes


class ICluster:

	def __init__(self, properties=None, name=None):
		try:
			with Ignis._pool.getClient() as client:
				if properties is None:
					if name is None:
						self._id = client.getIClusterService().newInstance0()
					else:
						self._id = client.getIClusterService().newInstance1a(name)
				else:
					if name is None:
						self._id = client.getIClusterService().newInstance1b(properties._id)
					else:
						self._id = client.getIClusterService().newInstance2(name, properties._id)
		except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
			raise IDriverException(ex.message, ex._cause)

	def setName(self, name):
		try:
			with Ignis._pool.getClient() as client:
				client.getIClusterService().setName(self._id, name)
		except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
			raise IDriverException(ex.message, ex._cause)

	def execute(self, *args):
		try:
			with Ignis._pool.getClient() as client:
				client.getIClusterService().execute(self._id, args)
		except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
			raise IDriverException(ex.message, ex._cause)

	def executeScript(self, script):
		try:
			with Ignis._pool.getClient() as client:
				client.getIClusterService().executeScript(self._id, script)
		except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
			raise IDriverException(ex.message, ex._cause)

	def sendFile(self, source, target):
		try:
			with Ignis._pool.getClient() as client:
				client.getIClusterService().sendFile(self._id, source, target)
		except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
			raise IDriverException(ex.message, ex._cause)

	def sendCompressedFile(self, source, target):
		try:
			with Ignis._pool.getClient() as client:
				client.getIClusterService().sendCompressedFile(self._id, source, target)
		except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
			raise IDriverException(ex.message, ex._cause)
