import sys
import logging
from ignis.executor.core import ILog
from ignis.executor.core.IExecutorData import IExecutorData
from ignis.executor.core.modules.ICacheContextModule import ICacheContextModule, ICacheContextModuleProcessor
from ignis.executor.core.modules.ICommModule import ICommModule, ICommModuleProcessor
from ignis.executor.core.modules.IExecutorServerModule import IExecutorServerModule, IExecutorServerModuleProcessor
from ignis.executor.core.modules.IGeneralActionModule import IGeneralActionModule, IGeneralActionModuleProcessor
from ignis.executor.core.modules.IGeneralModule import IGeneralModule, IGeneralModuleProcessor
from ignis.executor.core.modules.IIOModule import IIOModule, IIOModuleProcessor
from ignis.executor.core.modules.IMathModule import IMathModule, IMathModuleProcessor

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
