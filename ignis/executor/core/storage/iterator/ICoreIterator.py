class ICoreReadIterator:

	def next(self):
		pass

	def hasNext(self):
		pass

	def skip(self, n):
		while self.hasNext() and n > 0:
			self.next()
			n = n - 1


class ICoreWriteIterator:

	def write(self, obj):
		pass


def readToWrite(reader, writer, n=None):
	if n:
		while reader.hasNext() and n > 0:
			writer.write(reader.next())
			n = n - 1
	else:
		while reader.hasNext():
			writer.write(reader.next())


def iteratorToStr(reader):
	value = "["
	while reader.hasNext():
		value += str(reader.next())
		if reader.hasNext():
			value += ", "
	return value + "]"
