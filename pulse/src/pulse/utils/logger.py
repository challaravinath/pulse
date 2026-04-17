"""Logging setup"""
import logging
import sys

def setup_logging(level="INFO"):
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format='%(asctime)s | %(levelname)-8s | %(name)s | %(message)s',
        datefmt='%H:%M:%S',
        stream=sys.stdout
    )
    logging.getLogger('azure').setLevel(logging.WARNING)
