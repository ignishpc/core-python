

class IDriverContext:

	def parallelize(data, partitions):
		#TODO
		pass

	def collect(self, id):
		#TODO
		pass

	def collect1(self, id):
		return self.collect(id)[0];