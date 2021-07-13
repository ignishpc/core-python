
ARG REGISTRY=""
ARG TAG=""
FROM ${REGISTRY}ignishpc/common-builder${TAG}
ARG RELPATH=""

RUN export DEBIAN_FRONTEND=noninteractive && \
	apt update && \
	apt -y --no-install-recommends install \
		python3.8 \
		python3.8-dev \
		python3.8-distutils && \
	rm -rf /var/lib/apt/lists/* && \
	wget https://bootstrap.pypa.io/get-pip.py -O ${IGNIS_HOME}/bin/get-pip.py && \
	python3 ${IGNIS_HOME}/bin/get-pip.py

ENV MPI4PY_VERSION=3.0.3
RUN mkdir ${IGNIS_HOME}/core/python-libs && \
	cd ${IGNIS_HOME}/core/python-libs && \
	wget https://github.com/mpi4py/mpi4py/releases/download/${MPI4PY_VERSION}/mpi4py-${MPI4PY_VERSION}.tar.gz && \
	tar -xvf mpi4py-${MPI4PY_VERSION}.tar.gz && \
	rm -f mpi4py-${MPI4PY_VERSION}.tar.gz && \
	mv mpi4py-${MPI4PY_VERSION} mpi4py && \
	cd mpi4py && \
	export CC=${IGNIS_DEVEL}/mpi/bin/mpicc && \
	export CXX=${IGNIS_DEVEL}/mpi/bin/mpic++ && \
	export LIBRARY_PATH=${IGNIS_DEVEL}/mpi/lib && \
	export CPATH=${IGNIS_DEVEL}/mpi/include && \
	sed  "s/libraries():/libraries():\n    return []/g" -i setup.py && \
	python3 setup.py build  --mpicc=${IGNIS_DEVEL}/mpi/bin/mpicc

RUN cd ${IGNIS_HOME}/core/python-libs && \
	mkdir thrift && \
	cd thrift && \
	cp -R ${IGNIS_DEVEL}/thrift/lib/py/* . && \
	python3 setup.py build

COPY ${RELPATH}ignis-python-install.sh ${IGNIS_HOME}/bin
RUN chmod +x ${IGNIS_HOME}/bin/ignis-python-install.sh && \
	{ \
		echo '#!/bin/bash'; \
		echo 'exec python3 "${IGNIS_HOME}/core/python/ignis/Main.py" "$@"'; \
	} > ${IGNIS_HOME}/bin/ignis-python  && chmod +x ${IGNIS_HOME}/bin/ignis-python

COPY / ${IGNIS_HOME}/core/python
