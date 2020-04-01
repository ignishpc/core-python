from mpi4py import MPI
from ignis.executor.core.storage import IMemoryPartition, IRawMemoryPartition, IDiskPartition


class IMpi:

	def __init__(self, propertyParser, context):
		self.__propertyParser = propertyParser
		self.__context = context

	def gather(self, part, root):
		if self.executors() == 1: return
		if part.getType() == IMemoryPartition.TYPE:
			if part._IMemoryPartition__cls in (bytes, bytearray):
				pass
			elif part._IMemoryPartition__cls.__name__ == 'INumpyWrapper':
				pass
			else:
				pass
		elif part.getType() == IRawMemoryPartition.TYPE:
			pass
		else:  # IDiskPartition.TYP
			pass

	def bcast(self, part, root):
		if self.executors() == 1: return
		if part.type() == IMemoryPartition.TYPE:
			if part._IMemoryPartition__cls == bytearray:
				sz = self.native().bcast(part.size(), root)
				if not self.isRoot(root):
					part._IMemoryPartition__elements = bytearray(sz)

				with memoryview(part._IMemoryPartition__elements) as men:
					self.native().Bcast((men, men.nbytes, MPI.BYTE), root)

			elif part._IMemoryPartition__cls.__name__ == 'INumpyWrapperCl':
				sz = self.native().bcast(part.size(), root)
				elems = part._IMemoryPartition__elements
				if not self.isRoot(root):
					elems._resize(sz)
				self.native().Bcast((elems.array, elems.bytes(), MPI.BYTE), root)
			else:
				pass
		elif part.getType() == IRawMemoryPartition.TYPE:
			pass
		else:  # IDiskPartition.TYP
			pass

	def barrier(self):
		self.native().Barrier()

	def isRoot(self, root):
		return self.rank() == root

	def rank(self):
		return self.__context.executorId()

	def executors(self):
		return self.__context.executors()

	def native(self) -> MPI.Intracomm:
		return self.__context.mpiGroup()
