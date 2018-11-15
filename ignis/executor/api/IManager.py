from ignis.data.handle.IReader import IReader
from ignis.data.handle.IWriter import IWriter
from ignis.data.handle.INativeReader import INativeReader
from ignis.data.handle.INativeWriter import INativeWriter


class IManager:

	def __init__(self):
		self.writer = IWriter()
		self.reader = IReader()
		self.nativeReader = INativeReader()
		self.nativeWriter = INativeWriter()
