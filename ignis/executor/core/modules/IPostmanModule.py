import logging
import threading
import time
from ignis.rpc.executor.postman import IPostmanModule as IPostmanModuleRpc
from .IModule import IModule
from ignis.data.IObjectProtocol import IObjectProtocol
from thrift.transport.TTransport import TBufferedTransport
from ..IMessage import IMessage
from concurrent.futures.thread import ThreadPoolExecutor
from ignis.data.IFileTransport import IFileTransport
from ignis.data.ISocket import ISocket,IServerSocket

logger = logging.getLogger(__name__)


class IPostmanModule(IPostmanModuleRpc.Iface, IModule):

	def __init__(self, executorData):
		super().__init__(executorData)
		self.__started = False

	def __threadAccept(self, transport):
		try:
			protocol = IObjectProtocol(transport)
			id = protocol.readI64()
			addrMode = protocol.readString()
			if addrMode == "socket":
				pass  # o nothing else
			elif addrMode == "unixSocket":
				pass
			elif addrMode == "memoryBuffer":
				addrpath = protocol.readString()
				buffer1 = addrpath + "/buffer1"
				buffer2 = addrpath + "/buffer2"
				transport = IFileTransport(buffer1, buffer2, transport)
			else:
				raise ValueError("IPostmanModule id " + str(id) + " mode " + addrMode + " unknown")
			try:
				buffer = TBufferedTransport(transport)
				object = self.getIObject()
				logging.info("IPostmanModule id " + str(id) + " receiving" + " mode: " + addrMode)
				object.read(buffer)
				msg = IMessage("local", object)
				self._executorData.getPostBox().newInMessage(id, msg)
				logging.info("IPostmanModule id " + str(id) + " received OK")
				object.fit()
			except Exception as ex:
				logging.warning("IPostmanModule id " + str(id) + " received FAILS " + str(ex))

		except Exception as ex:
			logging.warning("IPostmanModule connection exception " + str(ex))

	def __threadServer(self, server):
		logging.info("IPostmanModule started")
		threads = list()
		server.listen()
		logging.info("IPostmanModule listening on port " + str(server.port))
		while self.__started:
			try:
				connection = server.accept()
				logging.info("IPostmanModule connection accepted")
				thread = threading.Thread(target=self.__threadAccept, args=(connection,))
				threads.append(thread)
				thread.start()
			except Exception as ex:
				if not self.__started:
					break
				logging.warning("IPostmanModule accept exception " + str(ex))
		for thread in threads:
			thread.join()

	def start(self):
		try:
			if not self.__started:
				self.__started = True
				logging.info("IPostmanModule starting")
				port = self._executorData.getParser().getInt("ignis.executor.transport.port")
				self.__server = IServerSocket(port=port)
				self.__threadServer = threading.Thread(target=self.__threadServer, args=(self.__server,))
				self.__threadServer.start()
		except Exception as ex:
			self.raiseRemote(ex)

	def stop(self):
		logging.info("IPostmanModule stopping")
		try:
			if self.__started:
				self.__started = False
				self.__server.close()
				self.__threadServer.join()
		except Exception as ex:
			self.raiseRemote(ex)

	def __send(self, id, msg, compression):
		try:
			addr = msg.getAddr()
			addrl = addr.split('!')
			addrMode = addrl[0]

			if addrMode == 'local':
				self._executorData.getPostBox().newInMessage(id, msg);
				return 0

			addrHost = addrl[1]
			addrPort = int(addrl[2])
			logging.info("IPostmanModule id " + str(id) + " connecting to " + addrHost + ":" + str(addrPort))
			transport = ISocket(host=addrHost, port=addrPort)
			for i in range(0, 10):
				try:
					transport.open()
					break
				except Exception as ex:
					time.sleep(1)
					if i == 9:
						raise ex
			protocol = IObjectProtocol(transport)
			protocol.writeI64(id)
			protocol.writeString(addrMode)
			if addrMode == "socket":
				pass  # o nothing else
			elif addrMode == "unixSocket":
				pass
			elif addrMode == "memoryBuffer":
				addrpath = addrl[3]
				addrBlockSize = int(addrl[4])
				protocol.writeString(addrpath)
				buffer1 = addrpath + "/buffer1"
				buffer2 = addrpath + "/buffer2"
				transport = IFileTransport(buffer1, buffer2, transport, addrBlockSize)
			else:
				raise ValueError("IPostmanModule id " + str(id) + " mode " + addrMode + " unknown")
			buffer = TBufferedTransport(transport)
			try:
				msg.getObj().write(buffer, compression)
			finally:
				transport.close()
			logging.info("IPostmanModule id " + str(id) + " sent OK")
			return 0
		except Exception as ex:
			logging.error("IPostmanModule id " + str(id) + " sent FAILS " + str(ex))
			return 1

	def __sendTry(self, id, msg, compression, tries):
		for cn in range(0, tries + 1):
			error = self.__send(id, msg, compression)
			if error == 0:
				return 0
		return 1

	def sendAll(self):
		try:
			msgs = self._executorData.getPostBox().popOutBox()
			if self._executorData.getParser().getString("ignis.executor.transport.threads") == "cores":
				threads = self._executorData.getParser().getInt("ignis.executor.cores")
			else:
				threads = self._executorData.getParser().getInt("ignis.executor.transport.threads")
			compression = self._executorData.getParser().getInt("ignis.executor.transport.compression")
			reconnections = self._executorData.getParser().getInt("ignis.executor.transport.reconnections")
			errors = 0
			with ThreadPoolExecutor(max_workers=threads) as executor:
				futures = list()
				for id, msg in msgs.items():
					future = executor.submit(self.__sendTry, id, msg, compression, reconnections)
					futures.append(future)
				for future in futures:
					errors += future.result()
			if errors > 0:
				logging.error("IPostmanModule fail to send " + str(errors) + " messages")
				raise RuntimeError("IPostmanModule fail to send " + str(errors) + " messages");

		except Exception as ex:
			self.raiseRemote(ex)

	def clearAll(self):
		try:
			self._executorData.getPostBox().popOutBox()
		except Exception as ex:
			self.raiseRemote(ex)
