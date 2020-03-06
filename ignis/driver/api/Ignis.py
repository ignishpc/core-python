import subprocess
import threading

from ignis.driver.core.IClientPool import IClientPool
from ignis.driver.core.ICallBack import ICallBack
from ignis.driver.api.IDriverException import IDriverException


class Ignis:
	__lock = threading.Lock()
	__backend = None
	_pool = None
	__callback = None

	@classmethod
	def start(Ignis):
		try:
			with Ignis.__lock:
				if not Ignis._pool:
					Ignis.__backend = subprocess.Popen(["ignis-backend"], stdout=subprocess.PIPE, stdin=subprocess.PIPE)

					backend_port = int(Ignis.__backend.stdout.readline())
					backend_compression = int(Ignis.__backend.stdout.readline())
					callback_port = int(Ignis.__backend.stdout.readline())
					callback_compression = int(Ignis.__backend.stdout.readline())

					Ignis.__callback = ICallBack(callback_port, callback_compression)
					Ignis._pool = IClientPool(backend_port, backend_compression)
		except Exception as ex:
			raise IDriverException(str(ex)) from ex

	@classmethod
	def stop(Ignis):
		try:
			with Ignis.__lock:
				if not Ignis._pool:
					return
			with Ignis._pool.getClient() as client:
				client.getIBackendService().stop()
			Ignis.__backend.wait()
			Ignis._pool.destroy()
			Ignis.__callback.stop()

			Ignis.__backend = None
			Ignis._pool = None
			Ignis.__callback = None
		except Exception as ex:
			raise IDriverException(ex) from ex

	@classmethod
	def _driverContext(Ignis):
		return Ignis.__callback.driverContext()
