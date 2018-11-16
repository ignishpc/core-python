import unittest
import coverage
import tempfile
import os
import sys



def main():
	tests = unittest.TestLoader().discover('.', pattern='*Test.py')
	cov = coverage.coverage(
		branch=True,
		include='../ignis/*.py',
	)
	cov.start()
	result = unittest.TextTestRunner(verbosity=2).run(tests)
	cov.stop()
	if result.wasSuccessful() and result.testsRun > 0:
		tmpdir = tempfile.gettempdir()
		covdir = os.path.join(tmpdir,"ignis-python-coverage")
		print('Coverage: (HTML version: file://%s/index.html)' % covdir, file=sys.stderr)
		cov.report(file=sys.stderr)
		cov.html_report(directory=covdir)


if __name__ == '__main__':
	main()
