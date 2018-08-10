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
    def reduceByKey(self, funct):
        """
        Parameters:
         - funct
        """
        pass

    def groupByKey(self):
        pass


class Client(Iface):
    def __init__(self, iprot, oprot=None):
        self._iprot = self._oprot = iprot
        if oprot is not None:
            self._oprot = oprot
        self._seqid = 0

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

    def groupByKey(self):
        self.send_groupByKey()
        self.recv_groupByKey()

    def send_groupByKey(self):
        self._oprot.writeMessageBegin('groupByKey', TMessageType.CALL, self._seqid)
        args = groupByKey_args()
        args.write(self._oprot)
        self._oprot.writeMessageEnd()
        self._oprot.trans.flush()

    def recv_groupByKey(self):
        iprot = self._iprot
        (fname, mtype, rseqid) = iprot.readMessageBegin()
        if mtype == TMessageType.EXCEPTION:
            x = TApplicationException()
            x.read(iprot)
            iprot.readMessageEnd()
            raise x
        result = groupByKey_result()
        result.read(iprot)
        iprot.readMessageEnd()
        if result.ex is not None:
            raise result.ex
        return


class Processor(Iface, TProcessor):
    def __init__(self, handler):
        self._handler = handler
        self._processMap = {}
        self._processMap["reduceByKey"] = Processor.process_reduceByKey
        self._processMap["groupByKey"] = Processor.process_groupByKey

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

    def process_groupByKey(self, seqid, iprot, oprot):
        args = groupByKey_args()
        args.read(iprot)
        iprot.readMessageEnd()
        result = groupByKey_result()
        try:
            self._handler.groupByKey()
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
        oprot.writeMessageBegin("groupByKey", msg_type, seqid)
        result.write(oprot)
        oprot.writeMessageEnd()
        oprot.trans.flush()

# HELPER FUNCTIONS AND STRUCTURES


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
                    self.funct = ignis.rpc.function.ttypes.ISourceFunction()
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
    (1, TType.STRUCT, 'funct', [ignis.rpc.function.ttypes.ISourceFunction, None], None, ),  # 1
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


class groupByKey_args(object):


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
        oprot.writeStructBegin('groupByKey_args')
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
all_structs.append(groupByKey_args)
groupByKey_args.thrift_spec = (
)


class groupByKey_result(object):
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
        oprot.writeStructBegin('groupByKey_result')
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
all_structs.append(groupByKey_result)
groupByKey_result.thrift_spec = (
    None,  # 0
    (1, TType.STRUCT, 'ex', [ignis.rpc.exception.ttypes.IRemoteException, None], None, ),  # 1
)
fix_spec(all_structs)
del all_structs

