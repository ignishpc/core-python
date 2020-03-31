from mpi4py import MPI
from ignis.executor.core.storage import IMemoryPartition, IRawMemoryPartition, IDiskPartition


class IMpi:

	def __init__(self, propertyParser, context):
		self.__propertyParser = propertyParser
		self.__context = context

	def gather(self, part, root):
		if self.executors() == 1: return
		if part.getType() == IMemoryPartition.TYPE:
			pass
		elif part.getType() == IRawMemoryPartition.TYPE:
			pass
		else:  # IDiskPartition.TYP
			pass

	def bcast(self, part, root):
		if self.executors() == 1: return
		if part.getType() == IMemoryPartition.TYPE:
			pass
		elif part.getType() == IRawMemoryPartition.TYPE:
			pass
		else:  # IDiskPartition.TYP
			pass

	def barrier(self):
		pass

	def isRoot(self, root):
		pass

	def rank(self):
		pass

	def executors(self):
		pass

	def native(self):
		pass
