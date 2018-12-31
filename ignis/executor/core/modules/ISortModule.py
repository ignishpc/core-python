import logging
from ignis.rpc.executor.sort import ISortModule as ISortModuleRpc
from .IModule import IModule
from ...api.function.IFunction2 import IFunction2


logger = logging.getLogger(__name__)

class ISortModule(IModule, ISortModuleRpc.Iface):

	def __init__(self, executorData):
		super().__init__(executorData)

	def localCustomSort(self, sf, ascending):
		try:
			less = self.loadSource(sf)
			self.mergeSort(less, ascending)
		except Exception as ex:
			self.raiseRemote(ex)

	def localSort(self, ascending):
		try:
			class NarutalOrder(IFunction2):

				def call(self, elem1, elem2, context):
					return elem1 < elem2

			self.mergeSort(NarutalOrder(), ascending)
		except Exception as ex:
			self.raiseRemote(ex)

	def mergeSort(self, less, ascending):
		logger.info("ISortModule sorting")
		#TODO
		logger.info("ISortModule sorted")
