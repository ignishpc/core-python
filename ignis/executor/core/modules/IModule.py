import traceback
import logging
from ignis.rpc.executor.exception.ttypes import IExecutorException

logger = logging.getLogger(__name__)


class IModule:

	def __init__(self, executor_data):
		self._executor_data = executor_data

	def _pack_exception(self, ex):
		message = str(ex)
		stack = ''.join(traceback.format_tb(ex.__traceback__))
		cause = ex.__class__.__name__ + ': ' + message + "\nCaused by: \n" + stack
		logger.error(cause)
		raise IExecutorException(message=message, cause=cause)

	def _use_source(self, src):
		try:
			self._executor_data.loadLibrary(src).before(self._executor_data.getContext())
		except Exception as ex:
			self._pack_exception(ex)
