

class IMessage:

	def __init__(self, addr, obj):
		self._addr = addr
		self._obj = obj

	def getAddr(self):
		return self._addr

	def getObject(self):
		return self._obj