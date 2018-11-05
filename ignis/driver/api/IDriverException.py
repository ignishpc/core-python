from ignis.rpc.exception.ttypes import IRemoteException


class IDriverException(RuntimeError):
	def __init__(self, ex):
		if type(ex) is IRemoteException:
			RuntimeError.__init__(self, self.__class__ + ": " + ex.message + "\n" + ex.stack)
		else:
			RuntimeError.__init__(self, str(ex))
