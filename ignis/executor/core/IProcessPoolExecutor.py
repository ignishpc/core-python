import cloudpickle
import concurrent.futures as futures


def _wrapper_send(fn, *args, **kwargs):
	return cloudpickle.dumps((fn, args, kwargs))


def _wrapper_rcv(bfn):
	fn, args, kwargs = cloudpickle.loads(bfn)
	result = fn(*args, **kwargs)
	if result is not None:
		return cloudpickle.dumps(result)


def _result(self):
	self.result = self.result2
	result = self.result()
	if result is not None:
		self._result = cloudpickle.loads(result)
	return self._result


class IProcessPoolExecutor(futures.ProcessPoolExecutor):

	def __init__(self, workers):
		super().__init__(workers)

	def __pathFuture(self, f):
		f.result2 = f.result
		f.result = lambda: _result(f)
		return f

	def submit(self, fn, *args, **kwargs):
		return self.__pathFuture(super().submit(_wrapper_rcv, _wrapper_send(fn, *args, **kwargs)))

	def map(self, fn, *args, **kwargs):
		return self.__pathFuture(super().map(_wrapper_rcv, _wrapper_send(fn, *args, **kwargs)))
