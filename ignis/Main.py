import logging
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
	if len(argv) < 3:
		logging.error("Executor need a server port and compression as argument")
		return 1
	try:
		port = int(argv[1])
		compression = int(argv[2])
	except ValueError as ex:
		logging.error("Executor need a valid server port and compression")
		return 1

	class IExecutorServerModuleImpl(IExecutorServerModule):

		def _createServices(self, processor):
			cache_context = ICacheContextModule(executor_data)
			processor.registerProcessor("ICacheContext", ICacheContextModuleProcessor(cache_context))
			comm = ICommModule(executor_data)
			processor.registerProcessor("IComm", ICommModuleProcessor(comm))
			general_action = IGeneralActionModule(executor_data)
			processor.registerProcessor("IGeneralActio", IGeneralActionModuleProcessor(general_action))
			general = IGeneralModule(executor_data)
			processor.registerProcessor("IGeneral", IGeneralModuleProcessor(general))
			io = IIOModule(executor_data)
			processor.registerProcessor("IIO", IIOModuleProcessor(io))
			math = IMathModule(executor_data)
			processor.registerProcessor("IMath", IMathModuleProcessor(math))

	executor_data = IExecutorData()
	server = IExecutorServerModuleImpl(executor_data)
	server.serve("IExecutorServer", port, compression)

	return 0


if __name__ == '__main__':
	sys.exit(main(sys.argv))
