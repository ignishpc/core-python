import json
import logging
import math
import os
import pathlib

from ignis.executor.api.IJsonValue import IJsonValue
from ignis.executor.core.io.IJsonWriter import IJsonWriter
from ignis.executor.core.modules.impl.IBaseImpl import IBaseImpl
from ignis.executor.core.storage import IDiskPartition

logger = logging.getLogger(__name__)


class IIOImpl(IBaseImpl):

    def __init__(self, executor_data):
        IBaseImpl.__init__(self, executor_data, logger)

    def partitionApproxSize(self):
        logger.info("IO: calculating partition size")
        input = self._executor_data.getPartitions()
        return sum(map(lambda p: p.bytes(), input))

    def __plainOrTextFile(self, path, minPartitions, createReader, delim, esize):
        with self.__openFileRead(path, binary=True) as file:
            readChunk = createReader(file)
            size = os.path.getsize(path)
            executorId = self._executor_data.getContext().executorId()
            executors = self._executor_data.getContext().executors()
            ex_chunk = int(size / executors)
            ex_chunk_init = executorId * ex_chunk
            ex_chunk_end = ex_chunk_init + ex_chunk
            minPartitionSize = self._executor_data.getProperties().partitionMinimal()
            minPartitions = math.ceil(minPartitions / executors)
            if len(delim) == 0:
                delim = b"\n"

            logger.info("IO: file has " + str(size) + " Bytes")

            if executorId > 0:
                padding = (ex_chunk_init - len(delim) - esize) if ex_chunk_init >= (
                        len(delim) + esize) else ex_chunk_init
                file.seek(padding)
                while True:
                    padding += len(readChunk())
                    if ex_chunk_init <= padding:
                        break

                ex_chunk_init = padding
                if executorId == executors - 1:
                    ex_chunk_end = size

            if ex_chunk / minPartitionSize < minPartitions:
                minPartitionSize = ex_chunk / minPartitions

            partitionGroup = self._executor_data.getPartitionTools().newPartitionGroup()
            self._executor_data.setPartitions(partitionGroup)
            partition = self._executor_data.getPartitionTools().newPartition()
            write_iterator = partition.writeIterator()
            partitionGroup.add(partition)
            partitionInit = ex_chunk_init
            filepos = ex_chunk_init
            elements = 0
            while filepos < ex_chunk_end:
                if (filepos - partitionInit) > minPartitionSize:
                    partition = self._executor_data.getPartitionTools().newPartition()
                    write_iterator = partition.writeIterator()
                    partitionGroup.add(partition)
                    partitionInit = filepos

                bb = readChunk()
                if bb[-len(delim):] == delim:
                    write_iterator.write(bb[:-len(delim)].decode("utf-8"))
                else:
                    write_iterator.write(bb.decode("utf-8"))
                elements += 1
                filepos += len(bb)
            ex_chunk_end = file.tell()

            logger.info("IO: created  " + str(len(partitionGroup)) + " partitions, " + str(elements) + " lines and " +
                        str(ex_chunk_end - ex_chunk_init) + " Bytes read ")

    def plainFile(self, path, minPartitions=1, delim='\n'):
        logger.info("IO: reading plain file")
        exs = list()
        esize = 0
        if "!" in delim:
            flag = 0
            while chr(flag) in delim:
                flag += 1
            ldelim = delim.replace("\\!", chr(flag))
            fields = ldelim.split("!")
            for i in range(len(fields)):
                fields[i] = fields[i].replace(chr(flag), "!")
                if i == 0:
                    delim = fields[0]
                else:
                    exs.append((fields[i]).encode("utf-8"))
            for ex in exs:
                if len(ex) > esize:
                    esize = len(ex)

        delim = delim.encode("utf-8")
        esize += len(delim)

        def createReader(file):
            buffer = b''

            def readChunk():
                nonlocal buffer
                chunk = b''
                while True:
                    part, found, buffer = buffer.partition(delim)
                    chunk += part
                    if found and len(exs) > 0:
                        for ex in exs:
                            if part.endswith(ex):
                                chunk += found
                                found = b""
                                break
                        if not found:
                            continue

                    if not found:
                        new_chunk = file.read(4096)
                        if not new_chunk:
                            return chunk
                        buffer += new_chunk
                    else:
                        return chunk + found

            return readChunk

        return self.__plainOrTextFile(path, minPartitions, createReader, delim, esize)

    def textFile(self, path, minPartitions=1):
        logger.info("IO: reading text file")
        return self.__plainOrTextFile(path, minPartitions, lambda file: file.readline, b'\n', 0)

    def partitionObjectFile(self, path, first, partitions):
        logger.info("IO: reading partitions object file")
        group = self._executor_data.getPartitionTools().newPartitionGroup(partitions)
        self._executor_data.setPartitions(group)

        for p in range(0, partitions):
            file_name = self.__partitionFileName(path, first + p)
            with self.__openFileRead(file_name) as file:  # Only to check
                pass
            file = IDiskPartition(file_name, 0, False, True, True)
            file.copyTo(group[p])

    def partitionTextFile(self, path, first, partitions):
        logger.info("IO: reading partitions text file")
        group = self._executor_data.getPartitionTools().newPartitionGroup()
        self._executor_data.setPartitions(group)

        for i in range(partitions):
            with self.__openFileRead(self.__partitionFileName(path, first + i)) as file:
                partition = self._executor_data.getPartitionTools().newPartition()
                write_iterator = partition.writeIterator()
                for line in file:
                    if line[-1] == '\n':
                        write_iterator.write(line[:-1])
                    else:
                        write_iterator.write(line)
                group.add(partition)

    def partitionJsonFile(self, path, first, partitions, objectMapping):
        logger.info("IO: reading partitions json file")
        group = self._executor_data.getPartitionTools().newPartitionGroup()
        self._executor_data.setPartitions(group)

        for i in range(partitions):
            with self.__openFileRead(self.__partitionFileName(path, first + i) + ".json") as file:
                partition = self._executor_data.getPartitionTools().newPartition()
                write_iterator = partition.writeIterator()
                if objectMapping:
                    for elem in json.load(file):
                        write_iterator.write(elem)
                else:
                    for elem in json.load(file):
                        write_iterator.write(IJsonValue(elem))

            group.add(partition)

    def saveAsObjectFile(self, path, compression, first):
        logger.info("IO: saving as object file")
        group = self._executor_data.getAndDeletePartitions()
        native = self._executor_data.getProperties().nativeSerialization()

        for i in range(len(group)):
            file_name = self.__partitionFileName(path, first + i)
            with self.__openFileWrite(file_name) as file:
                pass  # Only to check
            logger.info("IO: saving partition object file " + file_name)
            save = IDiskPartition(file_name, compression, native, True)
            group[i].copyTo(save)
            save.sync()
            group[i] = None

    def saveAsTextFile(self, path, first):
        logger.info("IO: saving as text file")
        group = self._executor_data.getAndDeletePartitions()

        for i in range(len(group)):
            file_name = self.__partitionFileName(path, first + i)
            with self.__openFileWrite(file_name) as file:
                logger.info("IO: saving text file " + file_name)
                for elem in group[i]:
                    print(elem, file=file)
                group[i] = None

    def saveAsJsonFile(self, path, first, pretty):
        logger.info("IO: saving as json file")
        group = self._executor_data.getAndDeletePartitions()

        for i in range(len(group)):
            file_name = self.__partitionFileName(path, first + i)
            with self.__openFileWrite(file_name + ".json") as file:
                logger.info("IO: saving json file " + file_name)
                json.dump(iter(group[i]), file, cls=IJsonWriter, indent=4 if pretty else None)
                group[i] = None

    def __partitionFileName(self, path, index):
        if not os.path.isdir(path):
            try:
                pathlib.Path(path).mkdir(parents=True, exist_ok=True)
            except Exception as ex:
                raise ValueError("Unable to create directory " + path + " " + str(ex))

        str_index = str(index)
        zeros = max(6 - len(str_index), 0)
        return path + "/part" + '0' * zeros + str_index

    def __openFileRead(self, path, binary=False):
        logger.info("IO: opening file " + path)
        if not os.path.exists(path):
            raise OSError(path + " was not found")

        try:
            file = open(path, "rb" if binary else "r")
        except Exception as ex:
            raise OSError(path + " cannot be opened")
        logger.info("IO: file opening successful")
        return file

    def __openFileWrite(self, path, binary=False):
        logger.info("IO: creating file " + path)
        if os.path.exists(path):
            if self._executor_data.getProperties().ioOverwrite():
                logger.warning("IO: " + path + " already exists")
                try:
                    os.remove(path)
                except Exception as ex:
                    raise OSError(path + " can not be removed")
            else:
                raise OSError(path + " already exists")
        try:
            file = open(path, "wb" if binary else "w")
        except Exception as ex:
            raise OSError(path + " cannot be opened")
        logger.info("IO: file created successful")
        return file
