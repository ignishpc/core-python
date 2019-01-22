import logging
import json
from ignis.rpc.executor.files import IFilesModule as IFilesModuleRpc
from .IModule import IModule

logger = logging.getLogger(__name__)


class IFilesModule(IModule, IFilesModuleRpc.Iface):

	def __init__(self, executorData):
		super().__init__(executorData)

	def readFile(self, path, offset, length, lines):
		try:
			obj = self.getIObject(elems=lines, bytes=length)
			logger.info(f"IFileModule reading, path: {path}, offset: {offset}, len: {length}, lines: {lines}")
			writer = obj.writeIterator()
			with open(path, encoding="utf-8") as file:
				file.seek(offset)
				for i in range(0, lines):
					writer.write(file.readline().rstrip('\n'))
			obj.fit()
			self._executorData.loadObject(obj)
			logger.info(f"IFileModule read")
		except Exception as ex:
			self.raiseRemote(ex)

	def saveFile(self, path, trunc, new_line):
		try:
			obj = self._executorData.loadObject()
			self._executorData.deleteLoadObject()
			size = len(obj)
			logger.info(f"IFileModule saving, path: {path}, truncate: {trunc}, new_line: {new_line}")

			if trunc:
				mode = "w"
			else:
				mode = "a"
			reader = obj.readIterator()
			with open(path, mode, encoding="utf-8") as file:
				if reader.hasNext():
					file.write(str(reader.next()))

				for i in range(0, size - 1):
					file.write("\n")
					file.write(str(reader.next()))

				if new_line:
					file.write("\n")
			logger.info(f"IFileModule saved")
		except Exception as ex:
			self.raiseRemote(ex)

	def saveJson(self, path, array_start, array_end):
		try:
			obj = self._executorData.loadObject()
			self._executorData.deleteLoadObject()
			size = len(obj)
			logger.info(f"IFileModule saving, path: {path}, array_start: {array_start}, array_end: {array_end}")

			if not array_start and not array_end:
				mode = "a"
			else:
				mode = "w"

			reader = obj.readIterator()
			with open(path, mode, encoding="utf-8") as file:
				if array_start:
					file.write("[\n")

				if reader.hasNext():
					json.dump(reader.next(), file)

				for i in range(0, size - 1):
					file.write(",\n")
					json.dump(reader.next(), file)

				if array_end:
					file.write("]")
			logger.info(f"IFileModule saved")
		except Exception as ex:
			self.raiseRemote(ex)
