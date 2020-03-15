import pickle


class INativeWriter:

	def write(self, protocol, obj):
		pickle.dump(obj=obj, file=protocol.trans, protocol=pickle.HIGHEST_PROTOCOL)
