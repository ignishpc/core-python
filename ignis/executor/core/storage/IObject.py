class IObject:

	def readIterator(self):
		pass

	def writeIterator(self):
		pass

	def read(self, trans):
		pass

	def write(self, trans, compression):
		pass

	def copyFrom(self, source):
		pass

	def copyTo(self, target):
		target.copyFrom(self)

	def moveFrom(self, source):
		pass

	def moveTo(self, target):
		target.moveFrom(self)

	def getSize(self):
		return len(self)

	def hasSize(self):
		return True

	def clear(self):
		pass

	def fit(self):
		pass

