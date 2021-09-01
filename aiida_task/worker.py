import logging
import os

import click

from aiida_task.cli import OPT_LOGLEVEL

WORKER_ID = f"{os.getuid()}-{os.getpid()}"
WORKER_LOGGER = logging.getLogger(f"aiida-{WORKER_ID}")


@click.command()
@click.option("--log-file")
@OPT_LOGLEVEL
def start_worker(log_file, log_level):
    """Start worker"""
    WORKER_LOGGER.setLevel(getattr(logging, log_level.upper()))
    WORKER_LOGGER.info("Started worker")
    # run forever
    while True:
        pass
