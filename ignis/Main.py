import sys
import logging
from thrift.TMultiplexedProcessor import TMultiplexedProcessor
from ignis.executor.core import ILog
from ignis.executor.core.IExecutorData import IExecutorData
from ignis.executor.core.modules.IFilesModule import IFilesModule, IFilesModuleRpc
from ignis.executor.core.modules.IKeysModule import IKeysModule, IKeysModuleRpc
from ignis.executor.core.modules.IMapperModule import IMapperModule, IMapperModuleRpc
from ignis.executor.core.modules.IPostmanModule import IPostmanModule, IPostmanModuleRpc
from ignis.executor.core.modules.IReducerModule import IReducerModule, IReducerModuleRpc
from ignis.executor.core.modules.IServerModule import IServerModule, IServerModuleRpc
from ignis.executor.core.modules.IShuffleModule import IShuffleModule, IShuffleModuleRpc
from ignis.executor.core.modules.ISortModule import ISortModule, ISortModuleRpc
from ignis.executor.core.modules.IStorageModule import IStorageModule, IStorageModuleRpc

logger = logging.getLogger(__name__)


def main(argv):
	ILog.init()

	processor = TMultiplexedProcessor()
	executorData = IExecutorData()

	files = IFilesModule(executorData)
	processor.registerProcessor("files", IFilesModuleRpc.Client(files))
	keys = IKeysModule(executorData)
	processor.registerProcessor("keys", IKeysModuleRpc.Client(keys))
	mapper = IMapperModule(executorData)
	processor.registerProcessor("mapper", IMapperModuleRpc.Client(mapper))
	postman = IPostmanModule(executorData)
	processor.registerProcessor("postman", IPostmanModuleRpc.Client(postman))
	reducer = IReducerModule(executorData)
	processor.registerProcessor("reducer", IReducerModuleRpc.Client(reducer))
	server = IServerModule(executorData)
	processor.registerProcessor("server", IServerModuleRpc.Client(server))
	shuffle = IShuffleModule(executorData)
	processor.registerProcessor("shuffle", IShuffleModuleRpc.Client(shuffle))
	sort = ISortModule(executorData)
	processor.registerProcessor("sort", ISortModuleRpc.Client(sort))
	storage = IStorageModule(executorData)
	processor.registerProcessor("storage", IStorageModuleRpc.Client(storage))

	if len(argv) == 1:
		logging.error("Executor need a server port as argument")
		return 1
	try:
		port = int(sys.argv[1])
	except ValueError as ex:
		logging.error("logging")
		return 1

	server.start(processor, port)
	return 0


if __name__ == '__main__':
	sys.exit(main(sys.argv))
