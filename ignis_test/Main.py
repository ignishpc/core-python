import unittest
import coverage
import os
import sys
import ignis.executor.core.ILog as Ilog
from mpi4py import MPI
from pathlib import Path


def main(path, parallel):
	tests = unittest.TestLoader().discover(path + '/executor/core/storage', pattern='*Test.py')
	if parallel:
		tests.addTests(unittest.TestLoader().discover(path + '/executor/core', pattern='IMpiTestParallel.py'))
	else:
		print("WARNING: mpi test skipped", sys.stderr)
	cov = coverage.coverage(
		branch=True,
		include=str(Path(path).parent) + '/ignis/*.py',
	)
	cov.start()
	result = unittest.TextTestRunner(verbosity=2).run(tests)
	cov.stop()
	if result.wasSuccessful() and result.testsRun > 0:
		covdir = os.path.join(os.getcwd(), "ignis-python-coverage")
		print('Coverage: (HTML version: file://%s/index.html)' % covdir, file=sys.stderr)
		cov.report(file=sys.stderr)
		cov.html_report(directory=covdir)


if __name__ == '__main__':
	Ilog.enable(False)
	rank = MPI.COMM_WORLD.Get_rank()
	parallel = MPI.COMM_WORLD.Get_size() > 1
	path = os.getcwd()
	Path("debug").mkdir(parents=True, exist_ok=True)
	os.chdir("debug")
	if parallel:
		wd = "np" + str(rank)
		Path(wd).mkdir(parents=True, exist_ok=True)
		os.chdir(wd)
		if rank > 0:
			log = open("log.txt", 'w')
			sys.stderr = log
			sys.stdout = log
	main(path, True)
	if rank > 0:
		sys.stderr.close()
