from ignis.executor.core.io.IReader import IReader, IEnumTypes, IReaderType
from ignis.executor.core.io.IWriter import IWriter
import numpy

IWriter[numpy.ndarray] = IWriter[IEnumTypes.I_LIST]
IWriter[numpy.bool_] = IWriter[IEnumTypes.I_BOOL]
IWriter[numpy.int8] = IWriter[IEnumTypes.I_I08]
IWriter[numpy.uint8] = IWriter[IEnumTypes.I_I16]
IWriter[numpy.int16] = IWriter[IEnumTypes.I_I16]
IWriter[numpy.uint16] = IWriter[IEnumTypes.I_I32]
IWriter[numpy.int32] = IWriter[IEnumTypes.I_I32]
IWriter[numpy.uint32] = IWriter[IEnumTypes.I_I64]
IWriter[numpy.int64] = IWriter[IEnumTypes.I_I64]
IWriter[numpy.uint64] = IWriter[IEnumTypes.I_I64]
IWriter[numpy.float16] = IWriter[IEnumTypes.I_DOUBLE]
IWriter[numpy.float32] = IWriter[IEnumTypes.I_DOUBLE]
IWriter[numpy.float64] = IWriter[IEnumTypes.I_DOUBLE]

__NUMPY_TYPES = {
	IEnumTypes.I_BOOL: numpy.bool_,
	IEnumTypes.I_I08: numpy.int8,
	IEnumTypes.I_I16: numpy.int16,
	IEnumTypes.I_I32: numpy.int32,
	IEnumTypes.I_I64: numpy.int64,
	IEnumTypes.I_DOUBLE: numpy.float64
}

def __readList(reader, protocol):
	size = reader.readSizeAux(protocol)
	tp = reader.readTypeAux(protocol)
	readerType = reader._getReaderType(tp)
	numpy_tp = __NUMPY_TYPES.get(tp, None)
	if numpy_tp is None:
		obj = list()
		for i in range(0, size):
			obj.append(readerType.read(reader, protocol))
	else:
		obj = numpy.zeros(shape=size, dtype=numpy_tp)
		for i in range(0, size):
			obj[i] = readerType.read(reader, protocol)
	return obj


IWriter[IEnumTypes.I_LIST] = IReaderType(__readList)
