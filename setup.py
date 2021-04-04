from setuptools import setup

setup(
	name='ignis-core',
	version='1.0',
	description='Ignis python core',
	packages=['ignis'],
	install_requires=['thrift', 'cloudpickle', 'mpi4py'],
	tests_require=['coverage']
)
