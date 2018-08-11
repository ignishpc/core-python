from ignis.rpc.exception.ttypes import IRemoteException

class IDriverException(RuntimeError):
	def __init__(self, ex):
		if type(ex) is IRemoteException:
			Exception.__init__(self, ex.message + "\n" + ex.stack)
		else:
			Exception.__init__(self)