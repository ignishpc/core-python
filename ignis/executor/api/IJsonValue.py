from ignis.executor.core.io.IWriter import IWriter
from ignis.executor.core.io.IReader import IReader


class IJsonValue:

    def __init__(self, value=None):
        self.__value = value

    def setNull(self):
        self.__value = None

    def setValue(self, value):
        self.__value = value

    def getValue(self):
        return self.__value

    def isNull(self):
        return self.__value is None

    def isBoolean(self):
        return type(self.__value) == bool

    def isInteger(self):
        return type(self.__value) == int

    def isDouble(self):
        return type(self.__value) == float

    def isNumber(self):
        return self.isInteger() or self.isDouble()

    def isString(self):
        return type(self.__value) == str

    def isArray(self) :
        return type(self.__value) == list

    def isMap(self) :
        return type(self.__value) == dict
