from mpi4py import MPI
from ignis.executor.core.storage import IMemoryPartition, IRawMemoryPartition, IDiskPartition
from ignis.executor.core.transport.IMemoryBuffer import IMemoryBuffer


class IMpi:

	def __init__(self, propertyParser, context):
		self.__propertyParser = propertyParser
		self.__context = context

	def gather(self, part, root):
		if self.executors() == 1: return
		if part.type() == IMemoryPartition.TYPE:
			if part._IMemoryPartition__cls in (bytes, bytearray):
				sz = part.size()
				szv = self.native().gather(sz, root)
				if self.isRoot(root):
					displs = self.__displs(szv)
					if root > 0:
						part._IMemoryPartition__elements = part._IMemoryPartition__elements.zfill(displs[root + 1])
					part._IMemoryPartition__elements.extend(bytes(displs[-1] - part.size()))
					with memoryview(part._IMemoryPartition__elements) as men:
						self.native().Gatherv(MPI.IN_PLACE, (men, szv, MPI.BYTE), root)
				else:
					with memoryview(part._IMemoryPartition__elements) as men:
						self.native().Gatherv((men, sz, MPI.BYTE), None, root)
			elif part._IMemoryPartition__cls.__name__ == 'INumpyWrapperCl':
				elems = part._IMemoryPartition__elements
				sz = part.size() * elems.itemsize()
				szv = self.native().gather(sz, root)

				if self.isRoot(root):
					displs = self.__displs(szv)
					elems._resize(int(displs[-1] / elems.itemsize()))
					if root > 0:
						elems[int(displs[root] / elems.itemsize()):sz] = elems[0:sz]
					self.native().Gatherv(MPI.IN_PLACE, (elems.array, szv, MPI.BYTE), root)
				else:
					self.native().Gatherv((elems.array, sz, MPI.BYTE), None, root)
			else:
				if not self.isRoot(root):
					buffer = IMemoryBuffer(part.bytes())
					part.write(buffer, self.__propertyParser.msgCompression())
					sz = buffer.writeEnd()
					buffer.resetBuffer()
				else:
					buffer = IMemoryBuffer(1)
					sz = 0
				szv = self.native().gather(sz, root)

				if self.isRoot(root):
					displs = self.__displs(szv)
					buffer.setBufferSize(displs[-1])
					self.native().Gatherv(MPI.IN_PLACE, (buffer.getBuffer().address(), szv, MPI.BYTE), root)
					rcv = IMemoryPartition(part._IMemoryPartition__native, part._IMemoryPartition__cls)
					for i in range(self.executors()):
						if i != root:
							buffer.setWriteBuffer(displs[i + 1])
							rcv.read(buffer)
						else:
							# Avoid serialization own elements
							part.moveTo(rcv)
					part.__dict__, rcv.__dict__ = rcv.__dict__, part.__dict__

				else:
					self.native().Gatherv((buffer.getBuffer().address(), sz, MPI.BYTE), None, root)
		elif part.type() == IRawMemoryPartition.TYPE:
			part.sync()
			buffer = part._transport
			sz = part.size()
			sz_bytes = buffer.writeEnd() - part._HEADER
			szv = self.native().gather((sz, sz_bytes), root)
			if self.isRoot(root):
				displs = self.__displs2(szv)
				part._elements = displs[-1][0]
				buffer.setBufferSize(displs[-1][1] + part._HEADER)
				buffer.setWriteBuffer(buffer.getBufferSize())
				if root > 0:
					buffer.getBuffer().padding(displs[root][1], sz_bytes)
				self.native().Gatherv(MPI.IN_PLACE,
				                      (buffer.getBuffer().address(part._HEADER), self.__getList(szv, 1), MPI.BYTE),
				                      root)
			else:
				self.native().Gatherv((buffer.getBuffer().address(part._HEADER), sz_bytes, MPI.BYTE), None, root)
		else:  # IDiskPartition.TYP
			part.sync()
			paths = self.native().gather(part.getPath(), root)
			if self.isRoot(root):
				path = part.getPath()
				part.rename(path + ".tmp")
				rcv = IDiskPartition(path, part._compression, part._native, persist=True, read=False)
				rcv._IDiskPartition__destroy = part._IDiskPartition__destroy
				part._IDiskPartition__destroy = True
				for i in range(len(paths)):
					if i != root:
						IDiskPartition(paths[i], part._compression, part._native, persist=True, read=True).copyTo(rcv)
					else:
						part.moveTo(rcv)
				part.__dict__, rcv.__dict__ = rcv.__dict__, part.__dict__

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
				buffer = IMemoryBuffer(part.bytes())
				if self.isRoot(root):
					part.write(buffer, self.__propertyParser.msgCompression())
				sz = self.native().bcast(buffer.writeEnd(), root)
				if not self.isRoot(root):
					buffer.setBufferSize(sz)

				self.native().Bcast((buffer.getBuffer().address(), sz, MPI.BYTE), root)
				if not self.isRoot(root):
					buffer.setWriteBuffer(sz)
					part.clear()
					part.read(buffer)

		elif part.type() == IRawMemoryPartition.TYPE:
			part.sync()
			buffer = part._transport
			elems, sz, header = self.native().bcast((part.size(),
			                                         buffer.writeEnd(),
			                                         part._header),
			                                        root)
			if not self.isRoot(root):
				part._elements = elems
				part._header = header
				buffer.setBufferSize(sz)
				buffer.setWriteBuffer(sz)
			self.native().Bcast((buffer.getBuffer().address(), sz, MPI.BYTE), root)

		else:  # IDiskPartition.TYP
			part.sync()
			path = self.native().bcast(part.getPath(), root)

			if not self.isRoot(root):
				rcv = IDiskPartition(path, part._compression, part._native, persist=True, read=True)
				part._IDiskPartition__destroy = True
				part.__dict__, rcv.__dict__ = rcv.__dict__, part.__dict__

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

	def __displs(self, szv):
		l = [0]
		v = l[0]
		for sz in szv:
			v += sz
			l.append(v)
		return l

	def __displs2(self, szv):
		l = [(0, 0)]
		v = l[0]
		for sz, sz_bytes in szv:
			v = v[0] + sz, v[1] + sz_bytes
			l.append(v)
		return l

	def __getList(self, szv, c):
		return list(map(lambda v: v[c], szv))
