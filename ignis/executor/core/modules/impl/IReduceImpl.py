import logging

from ignis.executor.core.modules.impl.IBaseImpl import IBaseImpl

logger = logging.getLogger(__name__)


class IReduceImpl(IBaseImpl):

	def __init__(self, executor_data):
		IBaseImpl.__init__(self, executor_data)
