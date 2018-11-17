from thrift.transport.TZlibTransport import TZlibTransport, BufferIO
import zlib

class IZlibTransport(TZlibTransport):

	def __init__(self, trans, compresslevel=9):
		super().__init__(trans, compresslevel)
		self.__trans = trans

	def readComp(self, sz):
		old = self.bytes_in
		while not super().readComp(max(sz, 256)) and old != self.bytes_in:
			old = self.bytes_in
		return True

	def flush(self):
		"""Flush any queued up data in the write buffer and ensure the
		compression buffer is flushed out to the underlying transport
		"""
		wout = self._TZlibTransport__wbuf.getvalue()
		if len(wout) > 0:
			zbuf = self._zcomp_write.compress(wout)
			self.bytes_out += len(wout)
			self.bytes_out_comp += len(zbuf)
		else:
			zbuf = b''#Fix thrift base error
		ztail = self._zcomp_write.flush(zlib.Z_SYNC_FLUSH)
		self.bytes_out_comp += len(ztail)
		if (len(zbuf) + len(ztail)) > 0:
			self._TZlibTransport__wbuf = BufferIO()
			self._TZlibTransport__trans.write(zbuf + ztail)
		self._TZlibTransport__trans.flush()

	def restart(self):
		super().__init__(self.__trans, self.compresslevel)
