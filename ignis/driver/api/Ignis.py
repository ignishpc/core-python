import subprocess
import threading

from ignis.driver.core.IClientPool import IClientPool
from ignis.driver.api.IDriverException import IDriverException


class Ignis:
	__lock = threading.Lock()
	__backend = None
	_pool = None

	@classmethod
	def debug(Ignis, port, host="127.0.0.1"):
		try:
			with Ignis.__lock:
				if not Ignis._pool:
					Ignis._pool = IClientPool(host, port)
		except Exception as ex:
			raise IDriverException() from ex

	@classmethod
	def start(Ignis):
		try:
			with Ignis.__lock:
				if not Ignis._pool:
					Ignis.__backend = subprocess.Popen(["ignis-backend"], stdout=subprocess.PIPE)
					Ignis._pool = IClientPool("127.0.0.1", int(Ignis.__backend.stdout.readline()))
		except Exception as ex:
			raise IDriverException(ex) from ex

	@classmethod
	def stop(Ignis):
		try:
			with Ignis.__lock:
				if Ignis._pool:
					if Ignis.__backend:
						try:
							with Ignis._pool.client() as client:
								client.getStopService().stop()
						except Exception:
							pass
						try:
							Ignis.__backend.wait(timeout=10)
						except subprocess.TimeoutExpired:
							Ignis.__backend.kill()
						Ignis.__backend = None
					Ignis._pool.destroy()
					Ignis._pool = None
		except Exception as ex:
			raise IDriverException(ex) from ex
