import logging
from ignis.rpc.executor.shuffle import IShuffleModule as IShuffleModuleRpc
from .IModule import IModule
from ..IMessage import IMessage
from ..storage.iterator.ICoreIterator import readToWrite

logger = logging.getLogger(__name__)


class IShuffleModule(IModule, IShuffleModuleRpc.Iface):

	def __init__(self, executorData):
		super().__init__(executorData)

	def createSplits(self, splits):
		try:
			logger.info("IShuffleModule started")
			obj = self._executorData.loadObject()
			self._executorData.deleteLoadObject()

			if len(splits) == 1 and splits[0].length == len(obj):
				self._executorData.getPostBox().newOutMessage(splits[0].msg_id, IMessage(splits[0].addr, obj))
				logger.info(f"IShuffleModule split addr: {splits[0].addr}, length: {splits[0].length}")

			reader = obj.readIterator()
			for split in splits:
				splitObj = self.getIObject(split.length)
				readToWrite(reader, splitObj.writeIterator(), split.length)
				self._executorData.getPostBox().newOutMessage(split.msg_id, IMessage(split.addr, splitObj))
				logger.info(f"IShuffleModule split addr: {split.addr}, length: {split.length}")
			logger.info("IShuffleModule finished")
		except Exception as ex:
			self.raiseRemote(ex)

	def joinSplits(self, order):
		try:
			logger.info("IShuffleModule joining splits")
			msgs = self._executorData.getPostBox().popInBox()

			obj = self.getIObject()
			for id in order:
				msgs[id].getObj().moveTo(obj)

			self._executorData.loadObject(obj)
			logger.info("IShuffleModule splits joined")
		except Exception as ex:
			self.raiseRemote(ex)
