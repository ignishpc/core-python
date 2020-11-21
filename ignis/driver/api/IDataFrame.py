import enum

import ignis.rpc.driver.exception.ttypes
from ignis.driver.api.IDriverException import IDriverException
from ignis.driver.api.ISource import ISource
from ignis.driver.api.Ignis import Ignis


class ICacheLevel(enum.Enum):
	NO_CACHE = 0
	PRESERVE = 1
	MEMORY = 2
	RAW_MEMORY = 3
	DISK = 4


class IDataFrame:

	def __init__(self, _id):
		self._id = _id

	def setName(self, name):
		try:
			with Ignis._pool.getClient() as client:
				self._id = client.getDataFrameService().setName(self._id, name)
		except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
			raise IDriverException(ex.message, ex._cause)

	def persist(self, cacheLevel):
		try:
			with Ignis._pool.getClient() as client:
				self._id = client.getDataFrameService().persist(self._id, cacheLevel.value)
		except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
			raise IDriverException(ex.message, ex._cause)

	def cache(self):
		try:
			with Ignis._pool.getClient() as client:
				self._id = client.getDataFrameService().cache(self._id)
		except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
			raise IDriverException(ex.message, ex._cause)

	def unpersist(self):
		try:
			with Ignis._pool.getClient() as client:
				self._id = client.getDataFrameService().unpersist(self._id)
		except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
			raise IDriverException(ex.message, ex._cause)

	def uncache(self):
		try:
			with Ignis._pool.getClient() as client:
				self._id = client.getDataFrameService().uncache(self._id)
		except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
			raise IDriverException(ex.message, ex._cause)

	def repartition(self, numPartitions):
		try:
			with Ignis._pool.getClient() as client:
				self._id = client.getDataFrameService().repartition(self._id, numPartitions)
		except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
			raise IDriverException(ex.message, ex._cause)

	def coalesce(self, numPartitions, shuffle):
		try:
			with Ignis._pool.getClient() as client:
				self._id = client.getDataFrameService().coalesce(self._id, numPartitions, shuffle)
		except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
			raise IDriverException(ex.message, ex._cause)

	def partitions(self):
		try:
			with Ignis._pool.getClient() as client:
				return client.getDataFrameService().partitions(self._id)
		except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
			raise IDriverException(ex.message, ex._cause)

	def saveAsObjectFile(self, path, compression=6):
		try:
			with Ignis._pool.getClient() as client:
				client.getDataFrameService().saveAsObjectFile(self._id, path, compression)
		except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
			raise IDriverException(ex.message, ex._cause)

	def saveAsTextFile(self, path):
		try:
			with Ignis._pool.getClient() as client:
				client.getDataFrameService().saveAsTextFile(self._id, path)
		except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
			raise IDriverException(ex.message, ex._cause)

	def saveAsJsonFile(self, path, pretty=True):
		try:
			with Ignis._pool.getClient() as client:
				client.getDataFrameService().saveAsJsonFile(self._id, path, pretty)
		except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
			raise IDriverException(ex.message, ex._cause)

	def map(self, src):
		try:
			with Ignis._pool.getClient() as client:
				return IDataFrame(client.getDataFrameService().map_(self._id, ISource.wrap(src).rpc()))
		except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
			raise IDriverException(ex.message, ex._cause)

	def filter(self, src):
		try:
			with Ignis._pool.getClient() as client:
				return IDataFrame(client.getDataFrameService().filter(self._id, ISource.wrap(src).rpc()))
		except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
			raise IDriverException(ex.message, ex._cause)

	def flatmap(self, src):
		try:
			with Ignis._pool.getClient() as client:
				return IDataFrame(client.getDataFrameService().flatmap(self._id, ISource.wrap(src).rpc()))
		except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
			raise IDriverException(ex.message, ex._cause)

	def mapPartitions(self, src, preservesPartitioning=True):
		try:
			with Ignis._pool.getClient() as client:
				return IDataFrame(client.getDataFrameService().mapPartitions(self._id, ISource.wrap(src).rpc(),
				                                                             preservesPartitioning))
		except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
			raise IDriverException(ex.message, ex._cause)

	def mapPartitionsWithIndex(self, src, preservesPartitioning=True):
		try:
			with Ignis._pool.getClient() as client:
				return IDataFrame(client.getDataFrameService().mapPartitionsWithIndex(self._id, ISource.wrap(src).rpc(),
				                                                                      preservesPartitioning))
		except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
			raise IDriverException(ex.message, ex._cause)

	def applyPartition(self, src):
		try:
			with Ignis._pool.getClient() as client:
				return IDataFrame(client.getDataFrameService().applyPartition(self._id, ISource.wrap(src).rpc()))
		except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
			raise IDriverException(ex.message, ex._cause)

	def groupBy(self, src, numPartitions=None):
		try:
			with Ignis._pool.getClient() as client:
				if numPartitions is None:
					return IDataFrame(client.getDataFrameService().groupBy(self._id, ISource.wrap(src).rpc()))
				else:
					return IDataFrame(
						client.getDataFrameService().groupBy2(self._id, ISource.wrap(src).rpc(), numPartitions))
		except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
			raise IDriverException(ex.message, ex._cause)

	def sort(self, ascending=True, numPartitions=None):
		try:
			with Ignis._pool.getClient() as client:
				if numPartitions is None:
					return IDataFrame(client.getDataFrameService().sort(self._id, ascending))
				else:
					return IDataFrame(client.getDataFrameService().sort2(self._id, ascending, numPartitions))
		except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
			raise IDriverException(ex.message, ex._cause)

	def sortBy(self, src, ascending=True, numPartitions=None):
		try:
			with Ignis._pool.getClient() as client:
				if numPartitions is None:
					return IDataFrame(client.getDataFrameService().sortBy(self._id, ISource.wrap(src).rpc(), ascending))
				else:
					return IDataFrame(client.getDataFrameService().sortBy3(self._id, ISource.wrap(src).rpc(), ascending,
					                                                       numPartitions))
		except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
			raise IDriverException(ex.message, ex._cause)

	def reduce(self, src):
		try:
			with Ignis._pool.getClient() as client:
				return Ignis._driverContext().collect1(
					client.getDataFrameService().reduce(self._id, ISource.wrap(src).rpc(), ISource("").rpc())  # TODO
				)
		except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
			raise IDriverException(ex.message, ex._cause)

	def treeReduce(self, src, depth=None):
		try:
			with Ignis._pool.getClient() as client:
				if depth is None:
					return Ignis._driverContext().collect1(  # TODO
						client.getDataFrameService().treeReduce(self._id, ISource.wrap(src).rpc(), ISource("").rpc())
					)
				else:
					return Ignis._driverContext().collect1(
						client.getDataFrameService().treeReduce4(self._id, ISource.wrap(src).rpc(), depth,
						                                         ISource("").rpc())  # TODO
					)
		except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
			raise IDriverException(ex.message, ex._cause)

	def collect(self):
		try:
			with Ignis._pool.getClient() as client:
				return Ignis._driverContext().collect(
					client.getDataFrameService().collect(self._id, ISource("").rpc())  # TODO
				)
		except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
			raise IDriverException(ex.message, ex._cause)

	def aggregate(self, seqOp, combOp):
		try:
			with Ignis._pool.getClient() as client:
				return Ignis._driverContext().collect1(
					client.getDataFrameService().aggregate(self._id, ISource.wrap(seqOp).rpc(),
					                                       ISource.wrap(combOp).rpc(),
					                                       ISource("").rpc())  # TODO
				)
		except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
			raise IDriverException(ex.message, ex._cause)

	def treeAggregate(self, seqOp, combOp, depth=None):
		try:
			with Ignis._pool.getClient() as client:
				if depth is None:
					return Ignis._driverContext().collect1(
						client.getDataFrameService().treeAggregate(self._id, ISource.wrap(seqOp).rpc(),
						                                           ISource.wrap(combOp).rpc(),
						                                           ISource("").rpc())  # TODO
					)
				else:
					return Ignis._driverContext().collect1(
						client.getDataFrameService().treeAggregate5(self._id, ISource.wrap(seqOp).rpc(),
						                                            depth,
						                                            ISource.wrap(combOp).rpc(),
						                                            ISource("").rpc())  # TODO
					)
		except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
			raise IDriverException(ex.message, ex._cause)

	def fold(self):
		try:
			with Ignis._pool.getClient() as client:
				return Ignis._driverContext().collect1(
					client.getDataFrameService().fold(self._id, ISource("").rpc())  # TODO
				)
		except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
			raise IDriverException(ex.message, ex._cause)

	def take(self, num):
		try:
			with Ignis._pool.getClient() as client:
				return Ignis._driverContext().collect(
					client.getDataFrameService().take(self._id, num, ISource("").rpc())  # TODO
				)
		except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
			raise IDriverException(ex.message, ex._cause)

	def foreach(self, src):
		try:
			with Ignis._pool.getClient() as client:
				client.getDataFrameService().foreach(self._id, ISource.wrap(src).rpc())
		except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
			raise IDriverException(ex.message, ex._cause)

	def foreachPartition(self, src):
		try:
			with Ignis._pool.getClient() as client:
				client.getDataFrameService().foreachPartition(self._id, ISource.wrap(src).rpc())
		except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
			raise IDriverException(ex.message, ex._cause)

	def top(self, num, cmp=None):
		try:
			with Ignis._pool.getClient() as client:
				if cmp is None:
					return Ignis._driverContext().collect1(
						client.getDataFrameService().top(self._id, num, ISource("").rpc())  # TODO
					)
				else:
					return Ignis._driverContext().collect1(  # TODO
						client.getDataFrameService().top(self._id, num, ISource.wrap(cmp).rpc(), ISource("").rpc())
					)
		except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
			raise IDriverException(ex.message, ex._cause)

	def sample(self, withReplacement, fraction, seed):
		try:
			with Ignis._pool.getClient() as client:
				return IDataFrame(client.getDataFrameService().sample(self._id, withReplacement, fraction, seed))
		except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
			raise IDriverException(ex.message, ex._cause)

	def takeSample(self, withReplacement, num, seed):
		try:
			with Ignis._pool.getClient() as client:
				return Ignis._driverContext().collect(  # TODO
					client.getDataFrameService().takeSample(self._id, withReplacement, num, seed, ISource("").rpc())
				)
		except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
			raise IDriverException(ex.message, ex._cause)

	def count(self):
		try:
			with Ignis._pool.getClient() as client:
				return client.getDataFrameService().count(self._id)
		except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
			raise IDriverException(ex.message, ex._cause)

	def max(self, cmp):
		try:
			with Ignis._pool.getClient() as client:
				return Ignis._driverContext().collect1(
					client.getDataFrameService().max(self._id, ISource.wrap(cmp).rpc(), ISource("").rpc())  # TODO
				)
		except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
			raise IDriverException(ex.message, ex._cause)

	def min(self, cmp):
		try:
			with Ignis._pool.getClient() as client:
				return Ignis._driverContext().collect1(
					client.getDataFrameService().min(self._id, ISource.wrap(cmp).rpc(), ISource("").rpc())  # TODO
				)
		except ignis.rpc.driver.exception.ttypes.IDriverException as ex:
			raise IDriverException(ex.message, ex._cause)
