import logging
import os
import subprocess
import sys
import threading

from ignis.driver.api.IDriverException import IDriverException
from ignis.driver.core.ICallBack import ICallBack
from ignis.driver.core.IClientPool import IClientPool

logger = logging.getLogger(__name__)


class Ignis:
    __lock = threading.Lock()
    __backend = None
    __pool = None
    __callback = None

    @classmethod
    def start(cls):
        try:
            with cls.__lock:
                if cls.__pool:
                    return
                transport_cmp = int(os.getenv("IGNIS_TRANSPORT_COMPRESSION", "0"))
                job_sockets = os.getenv("IGNIS_JOB_SOCKETS", "")
                cls.__backend = subprocess.Popen(["ignis-backend"], stdin=subprocess.PIPE)

                cls.__callback = ICallBack(os.path.join(job_sockets, "driver.sock"), transport_cmp)
                cls.__pool = IClientPool(os.path.join(job_sockets, "backend.sock"), transport_cmp)
        except Exception as ex:
            raise IDriverException(str(ex)) from ex

    @classmethod
    def stop(cls):
        try:
            with cls.__lock:
                if not cls.__pool:
                    return
                with cls.__pool.getClient() as client:
                    client.getBackendService().stop()
                cls.__pool.destroy()
                cls.__backend.wait()

                try:
                    cls.__callback.stop()
                except Exception as ex:
                    logger.error(str(ex))

                cls.__backend = None
                cls.__pool = None
                cls.__callback = None
        except Exception as ex:
            raise IDriverException(ex) from ex

    @classmethod
    def _clientPool(cls):
        if not cls.__pool:
            raise IDriverException("Ignis.start() must be invoked before the other routines")
        return cls.__pool

    @classmethod
    def _driverContext(cls):
        if not cls.__callback:
            raise IDriverException("Ignis.start() must be invoked before the other routines")
        return cls.__callback.driverContext()
