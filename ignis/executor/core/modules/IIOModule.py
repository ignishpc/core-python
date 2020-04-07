from ignis.executor.core.modules.IModule import IModule
from ignis.rpc.executor.io.IIOModule import Iface as IIOModuleIface, Processor as IIOModuleProcessor
from ignis.executor.core.storage import IDiskPartition
from ignis.executor.core.io.IJsonWriter import IJsonWriter
from ignis.executor.api.IJsonValue import IJsonValue
from pathlib import Path
import logging
import os
import math
import json

logger = logging.getLogger(__name__)


class IIOModule(IModule, IIOModuleIface):

	def __init__(self, executor_data):
		IModule.__init__(self, executor_data)

	def partitionCount(self):
		try:
			return len(self._executor_data.getPartitions())
		except Exception as ex:
			self._pack_exception(ex)

	def partitionApproxSize(self):
		try:
			group = self._executor_data.getPartitions()
			if len(group) == 0:
				return 0

			size = 0
			for part in group:
				size += part.bytes()
			return 0
		except Exception as ex:
			self._pack_exception(ex)

	def textFile(self, path):
		self.textFile2(path, 1)

	def textFile2(self, path, minPartitions):
		try:
			logger.info("IO: reading text file")
			with self.__openFileRead(path) as file:
				size = os.path.getsize(path)
				executorId = self._executor_data.getContext().executorId()
				executors = self._executor_data.getContext().executors()
				ex_chunk = size / executors
				ex_chunk_init = executorId * ex_chunk
				ex_chunk_end = ex_chunk_init + ex_chunk
				minPartitionSize = self._executor_data.getProperties().partitionMinimal()
				minPartitions = math.ceil(minPartitions / executors)

				logger.info("IO: file has " + str(size) + " Bytes")

				if executorId > 0:
					file.seek(ex_chunk_init - 1 if ex_chunk_init > 0 else ex_chunk_init)
					file.readline()
					ex_chunk_init = file.tell()
					if executorId == executors - 1:
						ex_chunk_end = size

				if ex_chunk / minPartitionSize < minPartitions:
					minPartitionSize = ex_chunk / minPartitions

				partitionGroup = self._executor_data.getPartitionTools().newPartitionGroup()
				partition = self._executor_data.getPartitionTools().newPartition < ()
				write_iterator = partition.writeIterator()
				partitionGroup.add(partition)
				partitionInit = ex_chunk_init
				elements = 0
				while file.tell() < ex_chunk_end:
					if (file.tell() - partitionInit) > minPartitionSize:
						partition = self._executor_data.getPartitionTools().newPartition()
						write_iterator = partition.writeIterator()
						partitionGroup.add(partition)
						partitionInit = file.tell()

					write_iterator.write(file.readline())
					elements += 1
				ex_chunk_end = file.tell()

				logger.info("IO: created  " + len(partitionGroup) + " partitions, " + elements + " lines and " +
				            str(ex_chunk_end - ex_chunk_init) + " Bytes read ")

			self._executor_data.setPartitions(partitionGroup)
		except Exception as ex:
			self._pack_exception(ex)

	def partitionObjectFile(self, path, first, partitions):
		try:
			logger.info("IO: reading partitions object file")
			group = self._executor_data.getPartitionTools().newPartitionGroup(partitions)
			for p in range(0, partitions):
				file_name = self.__partitionFileName(path, first + p)
				with self.__openFileRead(file_name) as file:  # Only to check
					pass
				file = IDiskPartition(file_name, 0, False, True, True)
				file.copyTo(group[p])
			self._executor_data.setPartitions(group)
		except Exception as ex:
			self._pack_exception(ex)

	def partitionObjectFile4(self, path, first, partitions, src):
		self._use_source(src)
		self.partitionObjectFile(path, first, partitions)

	def partitionTextFile(self, path, first, partitions):
		try:
			logger.info("IO: reading partitions text file")
			group = self._executor_data.getPartitionTools().newPartitionGroup()

			for i in range(partitions):
				with self.__openFileRead(self.__partitionFileName(path, first + i)) as file:
					partition = self._executor_data.getPartitionTools().newPartition()
					write_iterator = partition.writeIterator()
					for line in file:
						write_iterator.write(line)
					group.add(partition)
		except Exception as ex:
			self._pack_exception(ex)

	def partitionJsonFile4a(self, path, first, partitions, objectMapping):
		try:
			logger.info("IO: reading partitions json file")
			group = self._executor_data.getPartitionTools().newPartitionGroup()

			for i in range(partitions):
				with self.__openFileRead(self.__partitionFileName(path, first + i)) as file:
					partition = self._executor_data.getPartitionTools().newPartition()
					write_iterator = partition.writeIterator()
					if objectMapping:
						for elem in json.load(file):
							write_iterator.write(elem)
					else:
						for elem in json.load(file):
							write_iterator.write(IJsonValue(elem))

				group.add(partition)
		except Exception as ex:
			self._pack_exception(ex)

	def partitionJsonFile4b(self, path, first, partitions, src):
		self._use_source(src)
		self.partitionJsonFile4a(self, path, first, partitions, objectMapping=True)

	def saveAsObjectFile(self, path, compression, first):
		try:
			logger.info("IO: saving as object file")
			group = self._executor_data.getPartitions()
			self._executor_data.deletePartitions()
			native = self._executor_data.getProperties().nativeSerialization()

			for i in range(len(group)):
				file_name = self.__partitionFileName(path, first + i)
				with self.__openFileWrite(file_name) as file:
					pass  # Only to check
				logger.info("IO: saving partition object file " + file_name)
				save = IDiskPartition(file_name, compression, native, True)
				group[i].copyTo(save)
				save.sync()
		except Exception as ex:
			self._pack_exception(ex)

	def saveAsTextFile(self, path, first):
		try:
			logger.info("IO: saving as text file")
			group = self._executor_data.getPartitions()
			self._executor_data.deletePartitions()

			for i in range(len(group)):
				file_name = self.__partitionFileName(path, first + i)
				with self.__openFileWrite(file_name) as file:
					logger.info("IO: saving text file " + file_name)
					for elem in group[i]:
						file.write(elem.__repr__())
		except Exception as ex:
			self._pack_exception(ex)

	def saveAsJsonFile(self, path, first, pretty):
		try:
			logger.info("IO: saving as json file")
			group = self._executor_data.getPartitions()
			self._executor_data.deletePartitions()

			for i in range(len(group)):
				file_name = self.__partitionFileName(path, first + i)
				with self.__openFileWrite(file_name + ".json") as file:
					logger.info("IO: saving json file " + file_name)
					json.dump(iter(group[i]), file, cls=IJsonWriter, indent=4 if pretty else None)

			header = path + "/json"
			with self.__openFileWrite(header) as file:
				pass
			st = IDiskPartition(header, 0, False, True)
			st.sync()
		except Exception as ex:
			self._pack_exception(ex)

	def __partitionFileName(self, path, index):
		if not os.path.isdir(path):
			try:
				os.path.Path(path).mkdir(parents=True, exist_ok=True)
			except Exception as ex:
				raise ValueError("Unable to create directory " + path + " " + str(ex))

		str_index = str(index)
		zeros = max(6 - len(str_index), 0)
		return path + "/part" + '0' * zeros + str_index

	def __openFileRead(self, path, binary=False):
		logger.info("IO: opening file " + path)
		if not os.path.exists(path):
			raise OSError(path + " was not found")

		try:
			file = open(path, "rb" if binary else "r")
		except Exception as ex:
			raise OSError(path + " cannot be opened")
		logger.info("IO: file opening successful")
		return file

	def __openFileWrite(self, path, binary=False):
		logger.info("IO: creating file " + path)
		if os.path.exists(path):
			if self._executor_data.getProperties().ioOverwrite():
				logger.warning("IO: " + path + " already exists")
				try:
					os.remove(path)
				except Exception as ex:
					raise OSError(path + " can not be removed")
			else:
				raise OSError(path + " already exists")
		try:
			file = open(path, "wb" if binary else "w")
		except Exception as ex:
			raise OSError(path + " cannot be opened")
		logger.info("IO: file created successful")
		return file
