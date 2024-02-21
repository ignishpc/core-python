#!/bin/env bash
set -e

export DEBIAN_FRONTEND=noninteractive
apt update
apt -y --no-install-recommends install python3.12
if [ ! -f /usr/bin/python3 ]; then
  ln -s /usr/bin/python3.12 /usr/bin/python3
fi
rm -rf /var/lib/apt/lists/*

python3 ${IGNIS_HOME}/bin/get-pip.py --break-system-packages
rm -f ${IGNIS_HOME}/bin/get-pip.py
python3 -m pip install certifi numpy --break-system-packages

cd ${IGNIS_HOME}/core/python/
cd mpi4py
pip install . --break-system-packages
cd ..
cd thrift
pip install . --break-system-packages
cd ..
cd core
pip install . --break-system-packages
rm -fR ${IGNIS_HOME}/core/python/
