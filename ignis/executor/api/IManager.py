from ignis.data.handle.IReader import IReader
from ignis.data.handle.IWriter import IWriter


class IManager:

	def __init__(self) -> None:
		self.__writer = IWriter()
		self.__reader = IReader()

	def setReader(self, reader):
		self.__reader = reader

	def getReader(self):
		return self.__reader

	def setWriter(self, writer):
		self.__writer = writer

	def getWriter(self):
		return self.__writer