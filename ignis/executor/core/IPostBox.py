class IPostBox:

	def __init__(self):
		self.__inbox = dict()
		self.__outbox = dict()

	def getInBox(self):
		return self.__inbox

	def popInBox(self):
		inbox = self.__inbox
		self.__inbox = dict()
		return inbox

	def newInMessage(self, id, msg):
		self.__inbox[id] = msg

	def clearInBox(self):
		self.__inbox.clear()

	def getOutBox(self):
		return self.__outbox

	def popOutBox(self):
		outbox = self.__inbox
		self.__outbox = dict()
		return outbox

	def newOutMessage(self, id, msg):
		self.__outbox[id] = msg

	def clearOutBox(self):
		self.__outbox.clear()
