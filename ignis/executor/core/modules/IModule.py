import logging
import traceback
from ignis.rpc.exception.ttypes import IRemoteException

logger = logging.getLogger(__name__)


class IModule:

	def __init__(self, executorData):
		self._excutorData = executorData

	def raiseRemote(self, ex):
		raise IRemoteException(message=str(ex), stack=traceback.format_exc())
