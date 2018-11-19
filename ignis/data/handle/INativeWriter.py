import pickle


class INativeWriter:

	def write(self, object, transport):
		pickle.dump(obj=object, file=transport, protocol=pickle.HIGHEST_PROTOCOL)
