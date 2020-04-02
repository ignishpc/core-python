from ignis.executor.core.storage import IMemoryPartition, IRawMemoryPartition, IDiskPartition, IPartitionGroup
from pathlib import Path


class IPartitionTools:

	def __init__(self, propertyParser, context):
		self.__properties = propertyParser
		self.__context = context
		self.__partition_id_gen = 0

	def __preferredClass(self):
		return self.__context.vars().get('STORAGE_CLASS', list)

	def newPartition(self, other=None):
		partitionType = self.__properties.partitionType()
		if partitionType == IMemoryPartition.TYPE:
			return self.newMemoryPartition(other.size()) if other else self.newMemoryPartition()
		elif partitionType == IRawMemoryPartition.TYPE:
			return self.newRawMemoryPartition(other.bytes()) if other else self.newRawMemoryPartition()
		elif partitionType == IDiskPartition.TYPE:
			return self.newDiskPartition()
		else:
			raise ValueError("unknown partition type: " + partitionType)

	def newPartitionGroup(self, partitions=None):
		group = IPartitionGroup()
		if isinstance(partitions, int):
			for i in range(0, partitions):
				group.add(self.newPartition())
		else:
			for p in partitions:
				group.add(self.newPartition(p))
		return group

	def newMemoryPartition(self, elems=1000):
		if self.__preferredClass().__name__ == 'ndarray':
			return self.__newNumpyMemoryPartition(elems,
			                                      self.__properties.nativeSerialization(),
			                                      self.__context.vars()['STORAGE_CLASS_DTYPE'])
		return IMemoryPartition(native=self.__properties.nativeSerialization(),
		                        cls=self.__preferredClass())

	def newRawMemoryPartition(self, sz=10 * 1024 * 1024):
		return IRawMemoryPartition(bytes=sz,
		                           compression=self.__properties.partitionCompression(),
		                           native=self.__properties.nativeSerialization(),
		                           cls=self.__preferredClass())

	def newDiskPartition(self, name='', persist=False, read=False):
		path = self.__properties.jobDirectory() + "/partitions"
		self.createDirectoryIfNotExists(path)
		if name == '':
			path += "/partition"
			path += str(self.__context.executorId())
			path += "."
			path += str(self.__partition_id_gen)
			self.__partition_id_gen += 1
		else:
			path += '/' + name

		return IDiskPartition(path=path,
		                      compression=self.__properties.partitionCompression(),
		                      native=self.__properties.nativeSerialization(),
		                      persist=persist,
		                      read=read,
		                      cls=self.__preferredClass())

	def isMemory(self, part):
		return IMemoryPartition.TYPE == part.getType()

	def isRawMemory(self, part):
		return IRawMemoryPartition.TYPE == part.getType()

	def isDisk(self, part):
		return IDiskPartition.TYPE == part.getType()

	def createDirectoryIfNotExists(self, path):
		Path(path).mkdir(parents=True, exist_ok=True)

	@classmethod
	def __newNumpyMemoryPartition(cls, sz, native, dtype):
		from ignis.executor.core.io.INumpy import INumpyWrapper
		class INumpyWrapperCl(INumpyWrapper):
			def __init__(self):
				INumpyWrapper.__init__(self, sz, dtype)
				self.__class__ = INumpyWrapper

		return IMemoryPartition(native=native, cls=INumpyWrapperCl)
