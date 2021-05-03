import logging
import subprocess
import threading

from ignis.driver.api.IDriverException import IDriverException
from ignis.driver.core.ICallBack import ICallBack
from ignis.driver.core.IClientPool import IClientPool

logger = logging.getLogger(__name__)


class Ignis:
    __lock = threading.Lock()
    __backend = None
    _pool = None
    __callback = None

    @classmethod
    def start(cls):
        try:
            with cls.__lock:
                if cls._pool:
                    return
                cls.__backend = subprocess.Popen(["ignis-backend"], stdout=subprocess.PIPE, stdin=subprocess.PIPE)

                backend_port = int(cls.__backend.stdout.readline())
                backend_compression = int(cls.__backend.stdout.readline())
                callback_port = int(cls.__backend.stdout.readline())
                callback_compression = int(cls.__backend.stdout.readline())

                cls.__callback = ICallBack(callback_port, callback_compression)
                cls._pool = IClientPool(backend_port, backend_compression)
        except Exception as ex:
            raise IDriverException(str(ex)) from ex

    @classmethod
    def stop(cls):
        try:
            with cls.__lock:
                if not cls._pool:
                    return
                with cls._pool.getClient() as client:
                    client.getBackendService().stop()
                cls.__backend.wait()
                cls._pool.destroy()
                try:
                    cls.__callback.stop()
                except Exception as ex:
                    logger.error(str(ex))

                cls.__backend = None
                cls._pool = None
                cls.__callback = None
        except Exception as ex:
            raise IDriverException(ex) from ex

    @classmethod
    def _driverContext(cls):
        return cls.__callback.driverContext()
