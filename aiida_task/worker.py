import logging
import os
import socket

import click

from aiida_task.cli import OPT_LOGLEVEL

WORKER_ID = f"{os.getuid()}-{os.getpid()}"
WORKER_LOGGER = logging.getLogger(f"aiida-{WORKER_ID}")


@click.command()
@click.option("--fd")
@click.option("--log-file")
@OPT_LOGLEVEL
def start_worker(log_file, log_level, fd):
    """Start worker"""
    WORKER_LOGGER.setLevel(getattr(logging, log_level.upper()))
    WORKER_LOGGER.info("Started worker")

    # by default this is None, and raises on exception on OSX,
    # socket.error: [Errno 35] Resource temporarily unavailable
    # there is probably a better way to do this
    socket.setdefaulttimeout(100000000)

    WORKER_LOGGER.info("Connecting to file descriptor: %s", fd)
    sock = socket.fromfd(fd=int(fd), family=socket.AF_INET, type=socket.SOCK_STREAM)
    WORKER_LOGGER.info("Listening to socket: %s", sock.getsockname())

    # start watching the socket, dealing with one request at a time
    while True:
        conn, _ = sock.accept()
        request = conn.recv(1024)
        WORKER_LOGGER.info(str(request))
        # do something ....
        if WORKER_ID in request.decode("utf8"):
            WORKER_LOGGER.info("thats me!")
            conn.sendall(bytes(f"hallo from {WORKER_ID}", "utf-8"))
        conn.close()
