import logging
import os

import click

WORKER_LOGGER = logging.getLogger("aiida_worker")


@click.command()
def start_worker():
    """Start worker"""
    worker_id = (os.getuid(), os.getpid())
    # TODO proper logging
    print(f"Started worker: {worker_id}")
    # run forever
    while True:
        pass
