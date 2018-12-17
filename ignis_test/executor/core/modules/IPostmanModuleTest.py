import unittest
import random
from ignis.executor.core.modules.IPostmanModule import IPostmanModule
from ignis.executor.core.IExecutorData import IExecutorData
from ignis.executor.core.IMessage import IMessage


class IPostmanModuleTest(unittest.TestCase):

	def setUp(self):
		self.__executorData = IExecutorData()
		self.__postmanModule = IPostmanModule(self.__executorData)

		self.__executorData.getContext()["ignis.executor.storage"] = "raw memory"
		self.__executorData.getContext()["ignis.executor.cores"] = "1"
		self.__executorData.getContext()["ignis.transport.serialization"] = "ignis"
		self.__executorData.getContext()["ignis.executor.storage.compression"] = "6"
		self.__executorData.getContext()["ignis.executor.transport.port"] = "54321"
		self.__executorData.getContext()["ignis.executor.transport.threads"] = "4"
		self.__executorData.getContext()["ignis.executor.transport.compression"] = "1"
		self.__executorData.getContext()["ignis.executor.transport.reconnections"] = "0"

	def tearDown(self):
		pass

	def __test(self, addr):
		object = self.__postmanModule.getIObject()
		id = 2000

		writer = object.writeIterator()
		random.seed(0)
		input = [random.randint(0, 100) for i in range(0, 100)]
		for elem in input:
			writer.write(elem)
		self.__executorData.getPostBox().newOutMessage(id, IMessage(addr, object))
		self.__postmanModule.start()

		self.__postmanModule.sendAll()
		self.__postmanModule.stop()

		self.assert_(len(self.__executorData.getPostBox().getOutBox()) == 0)
		msgs = self.__executorData.getPostBox().popInBox()
		self.assert_(len(msgs) > 0)
		self.assert_(id in msgs)

		result = msgs[id].getObj()
		reader = result.readIterator()

		self.assertEqual(len(input), result.getSize())
		for elem in input:
			self.assertEqual(elem, reader.next())

	def test_local(self):
		self.__test("local")

	def test_socket(self):
		self.__test("socket!localhost!" + self.__executorData.getContext()["ignis.executor.transport.port"])

	def test_memoryBuffer(self):
		from tempfile import TemporaryDirectory
		with TemporaryDirectory() as temp_dir:
			path = temp_dir
			blockSize = str(10 * 1024)
			self.__test("memoryBuffer!localhost!" + self.__executorData.getContext()[
				"ignis.executor.transport.port"] + "!" + path + "!" + blockSize)
