import logging
import os.path
import sys

from ignis.executor.core import ILog
from ignis.executor.core.IExecutorData import IExecutorData
from ignis.executor.core.modules.ICacheContextModule import ICacheContextModule
from ignis.executor.core.modules.ICommModule import ICommModule
from ignis.executor.core.modules.IExecutorServerModule import IExecutorServerModule
from ignis.executor.core.modules.IGeneralActionModule import IGeneralActionModule
from ignis.executor.core.modules.IGeneralModule import IGeneralModule
from ignis.executor.core.modules.IIOModule import IIOModule
from ignis.executor.core.modules.IMathModule import IMathModule
from ignis.rpc.executor.cachecontext.ICacheContextModule import Processor as ICacheContextModuleProcessor
from ignis.rpc.executor.comm.ICommModule import Processor as ICommModuleProcessor
from ignis.rpc.executor.general.IGeneralModule import Processor as IGeneralModuleProcessor
from ignis.rpc.executor.general.action.IGeneralActionModule import Processor as IGeneralActionModuleProcessor
from ignis.rpc.executor.io.IIOModule import Processor as IIOModuleProcessor
from ignis.rpc.executor.math.IMathModule import Processor as IMathModuleProcessor

logger = logging.getLogger(__name__)


def main(argv):
	ILog.init()
	if len(argv) < 2:
		logging.error("Executor requires a socket address")
		return 1

	usock = argv[1]
	compression = int(os.getenv("IGNIS_TRANSPORT_COMPRESSION", "0"))

	class IExecutorServerModuleImpl(IExecutorServerModule):

		def __init__(self, executor_data):
			IExecutorServerModule.__init__(self, executor_data)

		def _createServices(self, processor):
			cache_context = ICacheContextModule(self._executor_data)
			processor.registerProcessor("ICacheContext", ICacheContextModuleProcessor(cache_context))
			comm = ICommModule(self._executor_data)
			processor.registerProcessor("IComm", ICommModuleProcessor(comm))
			general_action = IGeneralActionModule(self._executor_data)
			processor.registerProcessor("IGeneralAction", IGeneralActionModuleProcessor(general_action))
			general = IGeneralModule(self._executor_data)
			processor.registerProcessor("IGeneral", IGeneralModuleProcessor(general))
			io = IIOModule(self._executor_data)
			processor.registerProcessor("IIO", IIOModuleProcessor(io))
			math = IMathModule(self._executor_data)
			processor.registerProcessor("IMath", IMathModuleProcessor(math))

	executor_data = IExecutorData()
	server = IExecutorServerModuleImpl(executor_data)
	server.serve("IExecutorServer", usock, compression)

	return 0


if __name__ == '__main__':
	sys.exit(main(sys.argv))
