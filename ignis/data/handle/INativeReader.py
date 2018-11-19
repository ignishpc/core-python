import pickle


class INativeReader:

	class __Wrapper:

		def __init__(self, transport):
			self.__trans = transport

		def read(self, sz):
			return self.__trans.read(sz)

		def readline(self):
			buff = bytearray()
			while True:
				byte = self.__trans.read(1)
				buff.append(byte)
				if byte == b'\n':
					break
			return buff

	def read(self, transport):
		wrapper = INativeReader.__Wrapper(transport)
		return pickle.load(wrapper)
