#
# Autogenerated by Thrift Compiler (0.11.0)
#
# DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
#
#  options string: py
#

from thrift.Thrift import TType, TMessageType, TFrozenDict, TException, TApplicationException
from thrift.protocol.TProtocol import TProtocolException
from thrift.TRecursive import fix_spec

import sys
import logging
from .ttypes import *
from thrift.Thrift import TProcessor
from thrift.transport import TTransport
all_structs = []


class Iface(object):
    def getKeys(self):
        pass

    def getKeysWithCount(self):
        pass

    def prepareKeys(self, executorKeys):
        """
        Parameters:
         - executorKeys
        """
        pass

    def collectKeys(self):
        pass

    def reduceByKey(self, funct):
        """
        Parameters:
         - funct
        """
        pass


class Client(Iface):
    def __init__(self, iprot, oprot=None):
        self._iprot = self._oprot = iprot
        if oprot is not None:
            self._oprot = oprot
        self._seqid = 0

    def getKeys(self):
        self.send_getKeys()
        return self.recv_getKeys()

    def send_getKeys(self):
        self._oprot.writeMessageBegin('getKeys', TMessageType.CALL, self._seqid)
        args = getKeys_args()
        args.write(self._oprot)
        self._oprot.writeMessageEnd()
        self._oprot.trans.flush()

    def recv_getKeys(self):
        iprot = self._iprot
        (fname, mtype, rseqid) = iprot.readMessageBegin()
        if mtype == TMessageType.EXCEPTION:
            x = TApplicationException()
            x.read(iprot)
            iprot.readMessageEnd()
            raise x
        result = getKeys_result()
        result.read(iprot)
        iprot.readMessageEnd()
        if result.success is not None:
            return result.success
        if result.ex is not None:
            raise result.ex
        raise TApplicationException(TApplicationException.MISSING_RESULT, "getKeys failed: unknown result")

    def getKeysWithCount(self):
        self.send_getKeysWithCount()
        return self.recv_getKeysWithCount()

    def send_getKeysWithCount(self):
        self._oprot.writeMessageBegin('getKeysWithCount', TMessageType.CALL, self._seqid)
        args = getKeysWithCount_args()
        args.write(self._oprot)
        self._oprot.writeMessageEnd()
        self._oprot.trans.flush()

    def recv_getKeysWithCount(self):
        iprot = self._iprot
        (fname, mtype, rseqid) = iprot.readMessageBegin()
        if mtype == TMessageType.EXCEPTION:
            x = TApplicationException()
            x.read(iprot)
            iprot.readMessageEnd()
            raise x
        result = getKeysWithCount_result()
        result.read(iprot)
        iprot.readMessageEnd()
        if result.success is not None:
            return result.success
        if result.ex is not None:
            raise result.ex
        raise TApplicationException(TApplicationException.MISSING_RESULT, "getKeysWithCount failed: unknown result")

    def prepareKeys(self, executorKeys):
        """
        Parameters:
         - executorKeys
        """
        self.send_prepareKeys(executorKeys)
        self.recv_prepareKeys()

    def send_prepareKeys(self, executorKeys):
        self._oprot.writeMessageBegin('prepareKeys', TMessageType.CALL, self._seqid)
        args = prepareKeys_args()
        args.executorKeys = executorKeys
        args.write(self._oprot)
        self._oprot.writeMessageEnd()
        self._oprot.trans.flush()

    def recv_prepareKeys(self):
        iprot = self._iprot
        (fname, mtype, rseqid) = iprot.readMessageBegin()
        if mtype == TMessageType.EXCEPTION:
            x = TApplicationException()
            x.read(iprot)
            iprot.readMessageEnd()
            raise x
        result = prepareKeys_result()
        result.read(iprot)
        iprot.readMessageEnd()
        if result.ex is not None:
            raise result.ex
        return

    def collectKeys(self):
        self.send_collectKeys()
        self.recv_collectKeys()

    def send_collectKeys(self):
        self._oprot.writeMessageBegin('collectKeys', TMessageType.CALL, self._seqid)
        args = collectKeys_args()
        args.write(self._oprot)
        self._oprot.writeMessageEnd()
        self._oprot.trans.flush()

    def recv_collectKeys(self):
        iprot = self._iprot
        (fname, mtype, rseqid) = iprot.readMessageBegin()
        if mtype == TMessageType.EXCEPTION:
            x = TApplicationException()
            x.read(iprot)
            iprot.readMessageEnd()
            raise x
        result = collectKeys_result()
        result.read(iprot)
        iprot.readMessageEnd()
        if result.ex is not None:
            raise result.ex
        return

    def reduceByKey(self, funct):
        """
        Parameters:
         - funct
        """
        self.send_reduceByKey(funct)
        self.recv_reduceByKey()

    def send_reduceByKey(self, funct):
        self._oprot.writeMessageBegin('reduceByKey', TMessageType.CALL, self._seqid)
        args = reduceByKey_args()
        args.funct = funct
        args.write(self._oprot)
        self._oprot.writeMessageEnd()
        self._oprot.trans.flush()

    def recv_reduceByKey(self):
        iprot = self._iprot
        (fname, mtype, rseqid) = iprot.readMessageBegin()
        if mtype == TMessageType.EXCEPTION:
            x = TApplicationException()
            x.read(iprot)
            iprot.readMessageEnd()
            raise x
        result = reduceByKey_result()
        result.read(iprot)
        iprot.readMessageEnd()
        if result.ex is not None:
            raise result.ex
        return


class Processor(Iface, TProcessor):
    def __init__(self, handler):
        self._handler = handler
        self._processMap = {}
        self._processMap["getKeys"] = Processor.process_getKeys
        self._processMap["getKeysWithCount"] = Processor.process_getKeysWithCount
        self._processMap["prepareKeys"] = Processor.process_prepareKeys
        self._processMap["collectKeys"] = Processor.process_collectKeys
        self._processMap["reduceByKey"] = Processor.process_reduceByKey

    def process(self, iprot, oprot):
        (name, type, seqid) = iprot.readMessageBegin()
        if name not in self._processMap:
            iprot.skip(TType.STRUCT)
            iprot.readMessageEnd()
            x = TApplicationException(TApplicationException.UNKNOWN_METHOD, 'Unknown function %s' % (name))
            oprot.writeMessageBegin(name, TMessageType.EXCEPTION, seqid)
            x.write(oprot)
            oprot.writeMessageEnd()
            oprot.trans.flush()
            return
        else:
            self._processMap[name](self, seqid, iprot, oprot)
        return True

    def process_getKeys(self, seqid, iprot, oprot):
        args = getKeys_args()
        args.read(iprot)
        iprot.readMessageEnd()
        result = getKeys_result()
        try:
            result.success = self._handler.getKeys()
            msg_type = TMessageType.REPLY
        except TTransport.TTransportException:
            raise
        except ignis.rpc.exception.ttypes.IRemoteException as ex:
            msg_type = TMessageType.REPLY
            result.ex = ex
        except TApplicationException as ex:
            logging.exception('TApplication exception in handler')
            msg_type = TMessageType.EXCEPTION
            result = ex
        except Exception:
            logging.exception('Unexpected exception in handler')
            msg_type = TMessageType.EXCEPTION
            result = TApplicationException(TApplicationException.INTERNAL_ERROR, 'Internal error')
        oprot.writeMessageBegin("getKeys", msg_type, seqid)
        result.write(oprot)
        oprot.writeMessageEnd()
        oprot.trans.flush()

    def process_getKeysWithCount(self, seqid, iprot, oprot):
        args = getKeysWithCount_args()
        args.read(iprot)
        iprot.readMessageEnd()
        result = getKeysWithCount_result()
        try:
            result.success = self._handler.getKeysWithCount()
            msg_type = TMessageType.REPLY
        except TTransport.TTransportException:
            raise
        except ignis.rpc.exception.ttypes.IRemoteException as ex:
            msg_type = TMessageType.REPLY
            result.ex = ex
        except TApplicationException as ex:
            logging.exception('TApplication exception in handler')
            msg_type = TMessageType.EXCEPTION
            result = ex
        except Exception:
            logging.exception('Unexpected exception in handler')
            msg_type = TMessageType.EXCEPTION
            result = TApplicationException(TApplicationException.INTERNAL_ERROR, 'Internal error')
        oprot.writeMessageBegin("getKeysWithCount", msg_type, seqid)
        result.write(oprot)
        oprot.writeMessageEnd()
        oprot.trans.flush()

    def process_prepareKeys(self, seqid, iprot, oprot):
        args = prepareKeys_args()
        args.read(iprot)
        iprot.readMessageEnd()
        result = prepareKeys_result()
        try:
            self._handler.prepareKeys(args.executorKeys)
            msg_type = TMessageType.REPLY
        except TTransport.TTransportException:
            raise
        except ignis.rpc.exception.ttypes.IRemoteException as ex:
            msg_type = TMessageType.REPLY
            result.ex = ex
        except TApplicationException as ex:
            logging.exception('TApplication exception in handler')
            msg_type = TMessageType.EXCEPTION
            result = ex
        except Exception:
            logging.exception('Unexpected exception in handler')
            msg_type = TMessageType.EXCEPTION
            result = TApplicationException(TApplicationException.INTERNAL_ERROR, 'Internal error')
        oprot.writeMessageBegin("prepareKeys", msg_type, seqid)
        result.write(oprot)
        oprot.writeMessageEnd()
        oprot.trans.flush()

    def process_collectKeys(self, seqid, iprot, oprot):
        args = collectKeys_args()
        args.read(iprot)
        iprot.readMessageEnd()
        result = collectKeys_result()
        try:
            self._handler.collectKeys()
            msg_type = TMessageType.REPLY
        except TTransport.TTransportException:
            raise
        except ignis.rpc.exception.ttypes.IRemoteException as ex:
            msg_type = TMessageType.REPLY
            result.ex = ex
        except TApplicationException as ex:
            logging.exception('TApplication exception in handler')
            msg_type = TMessageType.EXCEPTION
            result = ex
        except Exception:
            logging.exception('Unexpected exception in handler')
            msg_type = TMessageType.EXCEPTION
            result = TApplicationException(TApplicationException.INTERNAL_ERROR, 'Internal error')
        oprot.writeMessageBegin("collectKeys", msg_type, seqid)
        result.write(oprot)
        oprot.writeMessageEnd()
        oprot.trans.flush()

    def process_reduceByKey(self, seqid, iprot, oprot):
        args = reduceByKey_args()
        args.read(iprot)
        iprot.readMessageEnd()
        result = reduceByKey_result()
        try:
            self._handler.reduceByKey(args.funct)
            msg_type = TMessageType.REPLY
        except TTransport.TTransportException:
            raise
        except ignis.rpc.exception.ttypes.IRemoteException as ex:
            msg_type = TMessageType.REPLY
            result.ex = ex
        except TApplicationException as ex:
            logging.exception('TApplication exception in handler')
            msg_type = TMessageType.EXCEPTION
            result = ex
        except Exception:
            logging.exception('Unexpected exception in handler')
            msg_type = TMessageType.EXCEPTION
            result = TApplicationException(TApplicationException.INTERNAL_ERROR, 'Internal error')
        oprot.writeMessageBegin("reduceByKey", msg_type, seqid)
        result.write(oprot)
        oprot.writeMessageEnd()
        oprot.trans.flush()

# HELPER FUNCTIONS AND STRUCTURES


class getKeys_args(object):


    def read(self, iprot):
        if iprot._fast_decode is not None and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None:
            iprot._fast_decode(self, iprot, [self.__class__, self.thrift_spec])
            return
        iprot.readStructBegin()
        while True:
            (fname, ftype, fid) = iprot.readFieldBegin()
            if ftype == TType.STOP:
                break
            else:
                iprot.skip(ftype)
            iprot.readFieldEnd()
        iprot.readStructEnd()

    def write(self, oprot):
        if oprot._fast_encode is not None and self.thrift_spec is not None:
            oprot.trans.write(oprot._fast_encode(self, [self.__class__, self.thrift_spec]))
            return
        oprot.writeStructBegin('getKeys_args')
        oprot.writeFieldStop()
        oprot.writeStructEnd()

    def validate(self):
        return

    def __repr__(self):
        L = ['%s=%r' % (key, value)
             for key, value in self.__dict__.items()]
        return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not (self == other)
all_structs.append(getKeys_args)
getKeys_args.thrift_spec = (
)


class getKeys_result(object):
    """
    Attributes:
     - success
     - ex
    """


    def __init__(self, success=None, ex=None,):
        self.success = success
        self.ex = ex

    def read(self, iprot):
        if iprot._fast_decode is not None and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None:
            iprot._fast_decode(self, iprot, [self.__class__, self.thrift_spec])
            return
        iprot.readStructBegin()
        while True:
            (fname, ftype, fid) = iprot.readFieldBegin()
            if ftype == TType.STOP:
                break
            if fid == 0:
                if ftype == TType.LIST:
                    self.success = []
                    (_etype10, _size7) = iprot.readListBegin()
                    for _i11 in range(_size7):
                        _elem12 = iprot.readI64()
                        self.success.append(_elem12)
                    iprot.readListEnd()
                else:
                    iprot.skip(ftype)
            elif fid == 1:
                if ftype == TType.STRUCT:
                    self.ex = ignis.rpc.exception.ttypes.IRemoteException()
                    self.ex.read(iprot)
                else:
                    iprot.skip(ftype)
            else:
                iprot.skip(ftype)
            iprot.readFieldEnd()
        iprot.readStructEnd()

    def write(self, oprot):
        if oprot._fast_encode is not None and self.thrift_spec is not None:
            oprot.trans.write(oprot._fast_encode(self, [self.__class__, self.thrift_spec]))
            return
        oprot.writeStructBegin('getKeys_result')
        if self.success is not None:
            oprot.writeFieldBegin('success', TType.LIST, 0)
            oprot.writeListBegin(TType.I64, len(self.success))
            for iter13 in self.success:
                oprot.writeI64(iter13)
            oprot.writeListEnd()
            oprot.writeFieldEnd()
        if self.ex is not None:
            oprot.writeFieldBegin('ex', TType.STRUCT, 1)
            self.ex.write(oprot)
            oprot.writeFieldEnd()
        oprot.writeFieldStop()
        oprot.writeStructEnd()

    def validate(self):
        return

    def __repr__(self):
        L = ['%s=%r' % (key, value)
             for key, value in self.__dict__.items()]
        return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not (self == other)
all_structs.append(getKeys_result)
getKeys_result.thrift_spec = (
    (0, TType.LIST, 'success', (TType.I64, None, False), None, ),  # 0
    (1, TType.STRUCT, 'ex', [ignis.rpc.exception.ttypes.IRemoteException, None], None, ),  # 1
)


class getKeysWithCount_args(object):


    def read(self, iprot):
        if iprot._fast_decode is not None and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None:
            iprot._fast_decode(self, iprot, [self.__class__, self.thrift_spec])
            return
        iprot.readStructBegin()
        while True:
            (fname, ftype, fid) = iprot.readFieldBegin()
            if ftype == TType.STOP:
                break
            else:
                iprot.skip(ftype)
            iprot.readFieldEnd()
        iprot.readStructEnd()

    def write(self, oprot):
        if oprot._fast_encode is not None and self.thrift_spec is not None:
            oprot.trans.write(oprot._fast_encode(self, [self.__class__, self.thrift_spec]))
            return
        oprot.writeStructBegin('getKeysWithCount_args')
        oprot.writeFieldStop()
        oprot.writeStructEnd()

    def validate(self):
        return

    def __repr__(self):
        L = ['%s=%r' % (key, value)
             for key, value in self.__dict__.items()]
        return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not (self == other)
all_structs.append(getKeysWithCount_args)
getKeysWithCount_args.thrift_spec = (
)


class getKeysWithCount_result(object):
    """
    Attributes:
     - success
     - ex
    """


    def __init__(self, success=None, ex=None,):
        self.success = success
        self.ex = ex

    def read(self, iprot):
        if iprot._fast_decode is not None and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None:
            iprot._fast_decode(self, iprot, [self.__class__, self.thrift_spec])
            return
        iprot.readStructBegin()
        while True:
            (fname, ftype, fid) = iprot.readFieldBegin()
            if ftype == TType.STOP:
                break
            if fid == 0:
                if ftype == TType.MAP:
                    self.success = {}
                    (_ktype15, _vtype16, _size14) = iprot.readMapBegin()
                    for _i18 in range(_size14):
                        _key19 = iprot.readI64()
                        _val20 = iprot.readI64()
                        self.success[_key19] = _val20
                    iprot.readMapEnd()
                else:
                    iprot.skip(ftype)
            elif fid == 1:
                if ftype == TType.STRUCT:
                    self.ex = ignis.rpc.exception.ttypes.IRemoteException()
                    self.ex.read(iprot)
                else:
                    iprot.skip(ftype)
            else:
                iprot.skip(ftype)
            iprot.readFieldEnd()
        iprot.readStructEnd()

    def write(self, oprot):
        if oprot._fast_encode is not None and self.thrift_spec is not None:
            oprot.trans.write(oprot._fast_encode(self, [self.__class__, self.thrift_spec]))
            return
        oprot.writeStructBegin('getKeysWithCount_result')
        if self.success is not None:
            oprot.writeFieldBegin('success', TType.MAP, 0)
            oprot.writeMapBegin(TType.I64, TType.I64, len(self.success))
            for kiter21, viter22 in self.success.items():
                oprot.writeI64(kiter21)
                oprot.writeI64(viter22)
            oprot.writeMapEnd()
            oprot.writeFieldEnd()
        if self.ex is not None:
            oprot.writeFieldBegin('ex', TType.STRUCT, 1)
            self.ex.write(oprot)
            oprot.writeFieldEnd()
        oprot.writeFieldStop()
        oprot.writeStructEnd()

    def validate(self):
        return

    def __repr__(self):
        L = ['%s=%r' % (key, value)
             for key, value in self.__dict__.items()]
        return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not (self == other)
all_structs.append(getKeysWithCount_result)
getKeysWithCount_result.thrift_spec = (
    (0, TType.MAP, 'success', (TType.I64, None, TType.I64, None, False), None, ),  # 0
    (1, TType.STRUCT, 'ex', [ignis.rpc.exception.ttypes.IRemoteException, None], None, ),  # 1
)


class prepareKeys_args(object):
    """
    Attributes:
     - executorKeys
    """


    def __init__(self, executorKeys=None,):
        self.executorKeys = executorKeys

    def read(self, iprot):
        if iprot._fast_decode is not None and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None:
            iprot._fast_decode(self, iprot, [self.__class__, self.thrift_spec])
            return
        iprot.readStructBegin()
        while True:
            (fname, ftype, fid) = iprot.readFieldBegin()
            if ftype == TType.STOP:
                break
            if fid == 1:
                if ftype == TType.LIST:
                    self.executorKeys = []
                    (_etype26, _size23) = iprot.readListBegin()
                    for _i27 in range(_size23):
                        _elem28 = IExecutorKeys()
                        _elem28.read(iprot)
                        self.executorKeys.append(_elem28)
                    iprot.readListEnd()
                else:
                    iprot.skip(ftype)
            else:
                iprot.skip(ftype)
            iprot.readFieldEnd()
        iprot.readStructEnd()

    def write(self, oprot):
        if oprot._fast_encode is not None and self.thrift_spec is not None:
            oprot.trans.write(oprot._fast_encode(self, [self.__class__, self.thrift_spec]))
            return
        oprot.writeStructBegin('prepareKeys_args')
        if self.executorKeys is not None:
            oprot.writeFieldBegin('executorKeys', TType.LIST, 1)
            oprot.writeListBegin(TType.STRUCT, len(self.executorKeys))
            for iter29 in self.executorKeys:
                iter29.write(oprot)
            oprot.writeListEnd()
            oprot.writeFieldEnd()
        oprot.writeFieldStop()
        oprot.writeStructEnd()

    def validate(self):
        return

    def __repr__(self):
        L = ['%s=%r' % (key, value)
             for key, value in self.__dict__.items()]
        return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not (self == other)
all_structs.append(prepareKeys_args)
prepareKeys_args.thrift_spec = (
    None,  # 0
    (1, TType.LIST, 'executorKeys', (TType.STRUCT, [IExecutorKeys, None], False), None, ),  # 1
)


class prepareKeys_result(object):
    """
    Attributes:
     - ex
    """


    def __init__(self, ex=None,):
        self.ex = ex

    def read(self, iprot):
        if iprot._fast_decode is not None and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None:
            iprot._fast_decode(self, iprot, [self.__class__, self.thrift_spec])
            return
        iprot.readStructBegin()
        while True:
            (fname, ftype, fid) = iprot.readFieldBegin()
            if ftype == TType.STOP:
                break
            if fid == 1:
                if ftype == TType.STRUCT:
                    self.ex = ignis.rpc.exception.ttypes.IRemoteException()
                    self.ex.read(iprot)
                else:
                    iprot.skip(ftype)
            else:
                iprot.skip(ftype)
            iprot.readFieldEnd()
        iprot.readStructEnd()

    def write(self, oprot):
        if oprot._fast_encode is not None and self.thrift_spec is not None:
            oprot.trans.write(oprot._fast_encode(self, [self.__class__, self.thrift_spec]))
            return
        oprot.writeStructBegin('prepareKeys_result')
        if self.ex is not None:
            oprot.writeFieldBegin('ex', TType.STRUCT, 1)
            self.ex.write(oprot)
            oprot.writeFieldEnd()
        oprot.writeFieldStop()
        oprot.writeStructEnd()

    def validate(self):
        return

    def __repr__(self):
        L = ['%s=%r' % (key, value)
             for key, value in self.__dict__.items()]
        return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not (self == other)
all_structs.append(prepareKeys_result)
prepareKeys_result.thrift_spec = (
    None,  # 0
    (1, TType.STRUCT, 'ex', [ignis.rpc.exception.ttypes.IRemoteException, None], None, ),  # 1
)


class collectKeys_args(object):


    def read(self, iprot):
        if iprot._fast_decode is not None and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None:
            iprot._fast_decode(self, iprot, [self.__class__, self.thrift_spec])
            return
        iprot.readStructBegin()
        while True:
            (fname, ftype, fid) = iprot.readFieldBegin()
            if ftype == TType.STOP:
                break
            else:
                iprot.skip(ftype)
            iprot.readFieldEnd()
        iprot.readStructEnd()

    def write(self, oprot):
        if oprot._fast_encode is not None and self.thrift_spec is not None:
            oprot.trans.write(oprot._fast_encode(self, [self.__class__, self.thrift_spec]))
            return
        oprot.writeStructBegin('collectKeys_args')
        oprot.writeFieldStop()
        oprot.writeStructEnd()

    def validate(self):
        return

    def __repr__(self):
        L = ['%s=%r' % (key, value)
             for key, value in self.__dict__.items()]
        return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not (self == other)
all_structs.append(collectKeys_args)
collectKeys_args.thrift_spec = (
)


class collectKeys_result(object):
    """
    Attributes:
     - ex
    """


    def __init__(self, ex=None,):
        self.ex = ex

    def read(self, iprot):
        if iprot._fast_decode is not None and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None:
            iprot._fast_decode(self, iprot, [self.__class__, self.thrift_spec])
            return
        iprot.readStructBegin()
        while True:
            (fname, ftype, fid) = iprot.readFieldBegin()
            if ftype == TType.STOP:
                break
            if fid == 1:
                if ftype == TType.STRUCT:
                    self.ex = ignis.rpc.exception.ttypes.IRemoteException()
                    self.ex.read(iprot)
                else:
                    iprot.skip(ftype)
            else:
                iprot.skip(ftype)
            iprot.readFieldEnd()
        iprot.readStructEnd()

    def write(self, oprot):
        if oprot._fast_encode is not None and self.thrift_spec is not None:
            oprot.trans.write(oprot._fast_encode(self, [self.__class__, self.thrift_spec]))
            return
        oprot.writeStructBegin('collectKeys_result')
        if self.ex is not None:
            oprot.writeFieldBegin('ex', TType.STRUCT, 1)
            self.ex.write(oprot)
            oprot.writeFieldEnd()
        oprot.writeFieldStop()
        oprot.writeStructEnd()

    def validate(self):
        return

    def __repr__(self):
        L = ['%s=%r' % (key, value)
             for key, value in self.__dict__.items()]
        return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not (self == other)
all_structs.append(collectKeys_result)
collectKeys_result.thrift_spec = (
    None,  # 0
    (1, TType.STRUCT, 'ex', [ignis.rpc.exception.ttypes.IRemoteException, None], None, ),  # 1
)


class reduceByKey_args(object):
    """
    Attributes:
     - funct
    """


    def __init__(self, funct=None,):
        self.funct = funct

    def read(self, iprot):
        if iprot._fast_decode is not None and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None:
            iprot._fast_decode(self, iprot, [self.__class__, self.thrift_spec])
            return
        iprot.readStructBegin()
        while True:
            (fname, ftype, fid) = iprot.readFieldBegin()
            if ftype == TType.STOP:
                break
            if fid == 1:
                if ftype == TType.STRUCT:
                    self.funct = ignis.rpc.source.ttypes.ISource()
                    self.funct.read(iprot)
                else:
                    iprot.skip(ftype)
            else:
                iprot.skip(ftype)
            iprot.readFieldEnd()
        iprot.readStructEnd()

    def write(self, oprot):
        if oprot._fast_encode is not None and self.thrift_spec is not None:
            oprot.trans.write(oprot._fast_encode(self, [self.__class__, self.thrift_spec]))
            return
        oprot.writeStructBegin('reduceByKey_args')
        if self.funct is not None:
            oprot.writeFieldBegin('funct', TType.STRUCT, 1)
            self.funct.write(oprot)
            oprot.writeFieldEnd()
        oprot.writeFieldStop()
        oprot.writeStructEnd()

    def validate(self):
        return

    def __repr__(self):
        L = ['%s=%r' % (key, value)
             for key, value in self.__dict__.items()]
        return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not (self == other)
all_structs.append(reduceByKey_args)
reduceByKey_args.thrift_spec = (
    None,  # 0
    (1, TType.STRUCT, 'funct', [ignis.rpc.source.ttypes.ISource, None], None, ),  # 1
)


class reduceByKey_result(object):
    """
    Attributes:
     - ex
    """


    def __init__(self, ex=None,):
        self.ex = ex

    def read(self, iprot):
        if iprot._fast_decode is not None and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None:
            iprot._fast_decode(self, iprot, [self.__class__, self.thrift_spec])
            return
        iprot.readStructBegin()
        while True:
            (fname, ftype, fid) = iprot.readFieldBegin()
            if ftype == TType.STOP:
                break
            if fid == 1:
                if ftype == TType.STRUCT:
                    self.ex = ignis.rpc.exception.ttypes.IRemoteException()
                    self.ex.read(iprot)
                else:
                    iprot.skip(ftype)
            else:
                iprot.skip(ftype)
            iprot.readFieldEnd()
        iprot.readStructEnd()

    def write(self, oprot):
        if oprot._fast_encode is not None and self.thrift_spec is not None:
            oprot.trans.write(oprot._fast_encode(self, [self.__class__, self.thrift_spec]))
            return
        oprot.writeStructBegin('reduceByKey_result')
        if self.ex is not None:
            oprot.writeFieldBegin('ex', TType.STRUCT, 1)
            self.ex.write(oprot)
            oprot.writeFieldEnd()
        oprot.writeFieldStop()
        oprot.writeStructEnd()

    def validate(self):
        return

    def __repr__(self):
        L = ['%s=%r' % (key, value)
             for key, value in self.__dict__.items()]
        return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not (self == other)
all_structs.append(reduceByKey_result)
reduceByKey_result.thrift_spec = (
    None,  # 0
    (1, TType.STRUCT, 'ex', [ignis.rpc.exception.ttypes.IRemoteException, None], None, ),  # 1
)
fix_spec(all_structs)
del all_structs

