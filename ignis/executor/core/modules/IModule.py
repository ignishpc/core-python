import traceback
from ignis.rpc.executor.exception.ttypes import IExecutorException


class IModule:

	def __init__(self, executor_data):
		self._executor_data = executor_data

	def _pack_exception(self, ex):
		message = str(ex)
		stack = ''.join(traceback.format_tb(ex.__traceback__))
		cause = ex.__class__.__name__ + ': ' + message + "\nCaused by: \n" + stack
		raise IExecutorException(message=message, cause = cause)


