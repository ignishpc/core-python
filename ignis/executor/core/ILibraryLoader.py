import importlib.util
import logging

import cloudpickle

logger = logging.getLogger(__name__)


class ILibraryLoader:

    def __init__(self):
        self.__functions = dict()

    def loadFuntion(self, name):
        if ":" not in name:
            if name in self.__functions:
                return self.__functions[name]()
            raise KeyError("Function " + name + " not found")
        try:
            values = name.split(":")
            path = values[0]
            className = values[1]
            spec = importlib.util.spec_from_file_location(name=className, location=path)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            classObject = getattr(module, className)
            self.__functions[className] = classObject
            return classObject()
        except Exception as ex:
            raise KeyError("Function " + name + " not found") from ex

    def loadLibrary(self, path):
        spec = importlib.util.spec_from_file_location(name="", location=path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        try:
            functions = getattr(module, '__ignis_library__')
        except TypeError as ex:
            raise KeyError("'_ ignis library _' is required to register a python file as ignis library")

        if not isinstance(functions, list):
            raise TypeError("'_ ignis library _' must be a list")

        for name in functions:
            try:
                self.__functions[name] = getattr(module, name)
            except TypeError as ex:
                logger.warning(name + " not found in " + path + " ignoring")

    @classmethod
    def unpickle(cls, bytes):
        return cloudpickle.loads(bytes)

    @classmethod
    def pickle(cls, src):
        return cloudpickle.dumps(src)
