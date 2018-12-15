from .ICoreIterator import ICoreReadIterator, ICoreWriteIterator, readToWrite
from ignis.data.IObjectProtocol import IObjectProtocol


class IRawWriteIterator(ICoreReadIterator):

	def __init__(self, raw):
		self._raw = raw
		self._writer = self._raw._writer
		self._type = self._raw._reader.getId() if self._raw._reader and not self._raw._native else None

	def write(self, obj):
		self._raw._elems += 1
		if self._raw._writer is None or self._raw._reader is None:
			if self._raw._native:
				self._raw._writer = self._raw._manager.nativeWriter
				self._raw._reader = self._raw._manager.nativeReader
				self._raw._protocol = self._raw._transport
			else:
				self._raw._writer = self._raw._manager.writer.getWriter(obj)
				self._raw._reader = self._raw._manager.reader.getReader(self._raw._writer.getId())
				self._raw._protocol = IObjectProtocol(self._raw._transport)
				self._type = self._raw._reader.getId()
			self._writer = self._raw._writer
		elif self._type and self._type != type(obj):
			raise ValueError("Current serialization does not support heterogeneous types")
		self._writer.write(obj, self._raw._protocol)


class IRawReadIterator(ICoreReadIterator):
	def __init__(self, raw):
		self._raw = raw
		self._elems = 0

	def hasNext(self):
		return self._elems < self._raw._elems

	def next(self):
		self._elems += 1
		return self._raw._reader.read(self._raw._protocol)
