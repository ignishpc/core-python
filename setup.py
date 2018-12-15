from setuptools import setup

setup(
	name='ignis',
	version='1.0',
	description='Ignis python core',
	packages=['ignis'],
	install_requires=['thrift==0.11.0','cloudpickle','cffi']
)

