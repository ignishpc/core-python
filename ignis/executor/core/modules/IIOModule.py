from ignis.executor.core.modules.IModule import IModule
from ignis.rpc.executor.io.IIOModule import Iface as IIOModuleIface, Processor as IIOModuleProcessor
from ignis.executor.core.storage import IDiskPartition
from pathlib import Path
import logging
import os
import math

logger = logging.getLogger(__name__)


class IIOModule(IModule, IIOModuleIface):

	def __init__(self, executor_data):
		IModule.__init__(self, executor_data)

	def partitionCount(self):
		try:
			raise NotImplementedError()
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
				file = IDiskPartition(file_name, 0, True, True)
				file.copyTo(group[p])
			self._executor_data.setPartitions(group)
		except Exception as ex:
			self._pack_exception(ex)

	def partitionObjectFile4(self, path, first, partitions, src):
		try:
			raise NotImplementedError()
		except Exception as ex:
			self._pack_exception(ex)

	def partitionTextFile(self, path, first, partitions):
		try:
			raise NotImplementedError()
		except Exception as ex:
			self._pack_exception(ex)

	def partitionJsonFile4a(self, path, first, partitions, objectMapping):
		try:
			raise NotImplementedError()
		except Exception as ex:
			self._pack_exception(ex)

	def partitionJsonFile4b(self, path, first, partitions, src):
		try:
			raise NotImplementedError()
		except Exception as ex:
			self._pack_exception(ex)

	def saveAsObjectFile(self, path, compression, first):
		try:
			raise NotImplementedError()
		except Exception as ex:
			self._pack_exception(ex)

	def saveAsTextFile(self, path, first):
		try:
			raise NotImplementedError()
		except Exception as ex:
			self._pack_exception(ex)

	def saveAsJsonFile(self, path, first, pretty):
		try:
			raise NotImplementedError()
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
