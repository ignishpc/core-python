import pickle


class INativeReader:

	class __Wrapper:

		def __init__(self, transport):
			self.__trans = transport

		def read(self, sz):
			return self.__trans.readAll(sz)

		def readline(self):
			buff = bytearray()
			while True:
				byte = self.__trans.read(1)
				buff.append(byte)
				if byte == b'\n':
					break
			return buff

	def read(self, protocol):
		wrapper = INativeReader.__Wrapper(protocol.trans)
		return pickle.load(wrapper)
