import random
from ignis.data.IZlibTransport import IZlibTransport
from ignis.data.IObjectProtocol import IObjectProtocol
from ignis.data.ISharedMemoryBuffer import ISharedMemoryBuffer
from ignis.executor.api.IManager import IManager


class IObjectTest():

	def getObject(self, elems, _bytes):
		raise NotImplementedError()

	def setUp(self):
		self._manager = IManager()
		self._object = self.getObject(100, 10 * 1024)

	def test_itWriteItRead(self):
		random.seed(0)
		examples = [random.randint(0, 100) for i in range(0, 100)]
		##########################################################################
		writer = self._object.writeIterator()
		for elem in examples:
			writer.write(elem)
		self.assertEqual(len(examples), self._object.getSize())
		##########################################################################
		reader = self._object.readIterator()
		for elem in examples:
			self.assert_(reader.hasNext())
			self.assertEqual(elem, reader.next())
		self.assert_(not reader.hasNext())

	def test_itWriteTransRead(self):
		random.seed(0)
		examples = [random.randint(0, 100) for i in range(0, 100)]
		##########################################################################
		writer = self._object.writeIterator()
		for elem in examples:
			writer.write(elem)
		self.assertEqual(len(examples), self._object.getSize())
		##########################################################################
		wBuffer = ISharedMemoryBuffer()
		self._object.write(wBuffer, 6)

		wTransport = IZlibTransport(wBuffer)
		wProtocol = IObjectProtocol(wTransport)
		result = wProtocol.readObject(self._manager)

		self.assertEqual(len(examples), len(result))
		for i, j in zip(examples, result):
			self.assertEqual(i, j)

	def test_transWriteItRead(self):
		random.seed(0)
		examples = [random.randint(0, 100) for i in range(0, 100)]
		##########################################################################
		rBuffer = ISharedMemoryBuffer()
		rTransport = IZlibTransport(rBuffer, 6)
		rProtocol = IObjectProtocol(rTransport)
		rProtocol.writeObject(examples, self._manager, native=False)
		rTransport.flush()
		self._object.read(rBuffer)
		self.assertEqual(len(examples), self._object.getSize())
		##########################################################################
		reader = self._object.readIterator()
		for elem in examples:
			self.assert_(reader.hasNext())
			self.assertEqual(elem, reader.next())
		self.assert_(not reader.hasNext())

	def test_transNativeWriteItRead(self):
		random.seed(0)
		examples = [random.randint(0, 100) for i in range(0, 100)]
		##########################################################################
		rBuffer = ISharedMemoryBuffer()
		rTransport = IZlibTransport(rBuffer, 6)
		rProtocol = IObjectProtocol(rTransport)
		rProtocol.writeObject(examples, self._manager, native=True, listHeader=True)
		rTransport.flush()
		self._object.read(rBuffer)
		self.assertEqual(len(examples), self._object.getSize())
		##########################################################################
		reader = self._object.readIterator()
		for elem in examples:
			self.assert_(reader.hasNext())
			self.assertEqual(elem, reader.next())
		self.assert_(not reader.hasNext())

	def test_transWriteTransRead(self):
		random.seed(0)
		examples = [random.randint(0, 100) for i in range(0, 100)]
		##########################################################################
		rBuffer = ISharedMemoryBuffer()
		rTransport = IZlibTransport(rBuffer, 6)
		rProtocol = IObjectProtocol(rTransport)
		rProtocol.writeObject(examples, self._manager, native=False)
		rTransport.flush()
		self._object.read(rBuffer)
		self.assertEqual(len(examples), self._object.getSize())
		##########################################################################
		wBuffer = ISharedMemoryBuffer()
		self._object.write(wBuffer, 6)

		wTransport = IZlibTransport(wBuffer)
		wProtocol = IObjectProtocol(wTransport)
		result = wProtocol.readObject(self._manager)

		self.assertEqual(len(examples), len(result))
		for i, j in zip(examples, result):
			self.assertEqual(i, j)

	def test_transNativeWriteTransRead(self):
		random.seed(0)
		examples = [random.randint(0, 100) for i in range(0, 100)]
		##########################################################################
		rBuffer = ISharedMemoryBuffer()
		rTransport = IZlibTransport(rBuffer, 6)
		rProtocol = IObjectProtocol(rTransport)
		rProtocol.writeObject(examples, self._manager, native=True, listHeader=True)
		rTransport.flush()
		self._object.read(rBuffer)
		self.assertEqual(len(examples), self._object.getSize())
		##########################################################################
		wBuffer = ISharedMemoryBuffer()
		self._object.write(wBuffer, 6)

		wTransport = IZlibTransport(wBuffer)
		wProtocol = IObjectProtocol(wTransport)
		result = wProtocol.readObject(self._manager)

		self.assertEqual(len(examples), len(result))
		for i, j in zip(examples, result):
			self.assertEqual(i, j)

	def test_clear(self):
		self.test_itWriteItRead()
		sz = self._object.getSize()
		self._object.clear()
		self.assertEqual(0, self._object.getSize())
		self.test_itWriteItRead()

	def test_append(self):
		random.seed(0)
		examples = [random.randint(0, 100) for i in range(0, 100)]
		examples2 = [random.randint(0, 100) for i in range(0, 100)]
		##########################################################################
		writer = self._object.writeIterator()
		for elem in examples:
			writer.write(elem)
		self.assertEqual(len(examples), self._object.getSize())
		##########################################################################

		##########################################################################
		writer = self._object.writeIterator()
		for elem in examples2:
			writer.write(elem)
		self.assertEqual(len(examples) + len(examples2), self._object.getSize())
		##########################################################################

		reader = self._object.readIterator()
		for elem in examples:
			self.assert_(reader.hasNext())
			self.assertEqual(elem, reader.next())
		for elem in examples2:
			self.assert_(reader.hasNext())
			self.assertEqual(elem, reader.next())
		self.assert_(not reader.hasNext())

	def test_copy(self):
		random.seed(0)
		examples = [random.randint(0, 100) for i in range(0, 100)]
		##########################################################################
		writer = self._object.writeIterator()
		for elem in examples:
			writer.write(elem)
		self.assertEqual(len(examples), self._object.getSize())
		##########################################################################
		copy = self.getObject(100, 10 * 1024)
		copy.copyFrom(self._object)
		self.assertEqual(len(examples), self._object.getSize())
		self.assertEqual(len(examples), copy.getSize())
		##########################################################################
		reader = self._object.readIterator()
		readerCopy = copy.readIterator()
		for elem in examples:
			self.assert_(reader.hasNext())
			self.assert_(readerCopy.hasNext())
			self.assertEqual(elem, reader.next())
			self.assertEqual(elem, readerCopy.next())
		self.assert_(not reader.hasNext())
		self.assert_(not readerCopy.hasNext())


	def test_move(self):
		random.seed(0)
		examples = [random.randint(0, 100) for i in range(0, 100)]
		##########################################################################
		writer = self._object.writeIterator()
		for elem in examples:
			writer.write(elem)
		self.assertEqual(len(examples), self._object.getSize())
		##########################################################################
		moved = self.getObject(100, 10 * 1024)
		moved.moveFrom(self._object)
		self.assertEqual(0, self._object.getSize())
		self.assertEqual(len(examples), moved.getSize())
		self._object = moved
		##########################################################################
		reader = self._object.readIterator()
		for elem in examples:
			self.assert_(reader.hasNext())
			self.assertEqual(elem, reader.next())
		self.assert_(not reader.hasNext())

	def tearDown(self):
		self._object.clear()
