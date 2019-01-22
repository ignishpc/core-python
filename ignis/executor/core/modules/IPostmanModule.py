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
from ignis.data.ISocket import ISocket, IServerSocket

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
				name = protocol.readString()
				transport.close()
				transport = ISocket(unix_socket=name)
				transport.open()
			elif addrMode == "memoryBuffer":
				addrpath = protocol.readString()
				buffer1 = addrpath + "/buffer1"
				buffer2 = addrpath + "/buffer2"
				transport = IFileTransport(buffer1, buffer2, transport)
			else:
				raise ValueError(f"IPostmanModule id {id} mode {addrMode} unknown")
			try:
				buffer = TBufferedTransport(transport)
				object = self.getIObject()
				logger.info(f"IPostmanModule id {id} receiving mode: {addrMode}")
				object.read(buffer)
				msg = IMessage("local", object)
				self._executorData.getPostBox().newInMessage(id, msg)
				logger.info(f"IPostmanModule id {id} received OK")
				object.fit()
			except Exception as ex:
				logger.warning(f"IPostmanModule id {id} received FAILS {ex}")

		except Exception as ex:
			logger.warning(f"IPostmanModule connection exception {ex}")

	def __threadServer(self, server):
		logger.info("IPostmanModule started")
		threads = list()
		server.listen()
		logger.info(f"IPostmanModule listening on port {server.port}")
		while self.__started:
			try:
				connection = server.accept()
				# Conection to unlock thread
				if not self.__started:
					connection.close()
					break
				logger.info("IPostmanModule connection accepted")
				thread = threading.Thread(target=self.__threadAccept, args=(connection,))
				threads.append(thread)
				thread.start()
			except Exception as ex:
				logger.warning(f"IPostmanModule accept exception {ex}")
		self.__server.close()
		for thread in threads:
			thread.join()

	def start(self):
		try:
			if not self.__started:
				self.__started = True
				logger.info("IPostmanModule starting")
				port = self._executorData.getParser().getInt("ignis.transport.port")
				self.__server = IServerSocket(port=port)
				self.__threadServer = threading.Thread(target=self.__threadServer, args=(self.__server,))
				self.__threadServer.start()
		except Exception as ex:
			self.raiseRemote(ex)

	def stop(self):
		logger.info("IPostmanModule stopping")
		try:
			if self.__started:
				self.__started = False
				try:
					aux = ISocket(host="localhost", port=self.__server.port)
					aux.open()
					aux.close()
				except Exception as ex:
					logger.warning("Fails to interrupt the server")
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
			logger.info(f"IPostmanModule id {id} connecting to {addrHost}:{addrPort}")
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
				ss = IServerSocket(unix_socket=addrl[3])
				ss.listen()
				protocol.writeString(addrl[3])
				transport.close()
				ss.handle.settimeout(10)
				transport = ss.accept()
				ss.close()
			elif addrMode == "memoryBuffer":
				addrpath = addrl[3]
				addrBlockSize = int(addrl[4])
				protocol.writeString(addrpath)
				buffer1 = addrpath + "/buffer1"
				buffer2 = addrpath + "/buffer2"
				transport = IFileTransport(buffer1, buffer2, transport, addrBlockSize)
			else:
				raise ValueError(f"IPostmanModule id {id} mode: {addrMode} unknown")
			buffer = TBufferedTransport(transport)
			try:
				msg.getObj().write(buffer, compression)
			finally:
				transport.close()
			logger.info(f"IPostmanModule id {id} sent OK")
			return 0
		except Exception as ex:
			logger.error(f"IPostmanModule id {id} sent FAILS {ex}")
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
			if self._executorData.getParser().getString("ignis.transport.threads") == "cores":
				threads = self._executorData.getParser().getInt("ignis.executor.cores")
			else:
				threads = self._executorData.getParser().getInt("ignis.transport.threads")
			compression = self._executorData.getParser().getInt("ignis.transport.compression")
			reconnections = self._executorData.getParser().getInt("ignis.transport.reconnections")
			errors = 0
			with ThreadPoolExecutor(max_workers=threads) as executor:
				futures = list()
				for id, msg in msgs.items():
					future = executor.submit(self.__sendTry, id, msg, compression, reconnections)
					futures.append(future)
				for future in futures:
					errors += future.result()
			if errors > 0:
				logger.error(f"IPostmanModule fail to send {errors} messages")
				raise RuntimeError(f"IPostmanModule fail to send {errors} messages");

		except Exception as ex:
			self.raiseRemote(ex)

	def clearAll(self):
		try:
			self._executorData.getPostBox().popOutBox()
		except Exception as ex:
			self.raiseRemote(ex)
