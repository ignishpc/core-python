import ignis.executor.core.ILog
import sys
import logging

logger = logging.getLogger(__name__)

def main(argv):
	logger.info("hola")
	logger.error("adios")

if __name__ == '__main__':
	main(sys.argv)