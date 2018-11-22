import os
import multiprocessing


class IParallelFork:
	class __FastSingle:

		def __enter__(self):
			return self

		def __exit__(self, exc_type, exc_val, exc_tb):
			pass

		def wait(self):
			pass

	def __init__(self, workers=0):
		self.__workers = workers - 1
		self.__id = 0
		if self.__workers > 0:
			self.__lock = multiprocessing.Lock()
			self.__barrier = multiprocessing.Barrier(parties=workers)
		else:
			self.__lock = IParallelFork.__FastSingle()
			self.__barrier = self.__lock

	def isMaster(self):
		return self.__id == 0

	def workers(self):
		return self.__workers

	def isWorker(self):
		return self.__id > 0

	def getId(self):
		return self.__id

	def barrier(self):
		self.__barrier.wait()

	def critical(self):
		return self.__lock

	def __enter__(self):
		self.__childs = list()
		for i in range(0, self.__workers):
			childId = len(self.__childs) + 1
			pid = os.fork()
			self.__childs.append(pid)
			if pid == 0:
				self.__id = childId
				self.__childs = None
				break
		return self

	def __exit__(self, exc_type, exc_val, exc_tb):
		if self.isWorker():
			os._exit(0)
		errors = 0
		for child in self.__childs:
			pid, code = os.waitpid(child, 0)
			errors += 0 if code == 0 else 1
		if errors > 0:
			raise RuntimeError("One process finished with error exit code")
