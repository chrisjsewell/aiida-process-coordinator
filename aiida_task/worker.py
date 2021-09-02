import logging
import os
import socket
import threading
import time

import click

from .cli import OPT_LOGLEVEL
from .shared import HEARTBEAT_TIMEOUT_MS, SOCKET_FAMILY, SOCKET_TYPE, get_socket_from_fd

WORKER_ID = f"{os.getuid()}-{os.getpid()}"
WORKER_LOGGER = logging.getLogger(f"worker-{WORKER_ID}")


@click.command()
@click.option("--fd")
@OPT_LOGLEVEL
def start_worker(log_level, fd):
    """Start a worker"""
    WORKER_LOGGER.setLevel(getattr(logging, log_level.upper()))
    WORKER_LOGGER.info("Started")

    sock = get_socket_from_fd(int(fd))
    conn = socket.socket(family=SOCKET_FAMILY, type=SOCKET_TYPE)
    conn.connect(sock.getsockname())
    WORKER_LOGGER.info("Connected to server: %s", sock.getsockname())

    # start thread for sending heartbeat
    thread = threading.Thread(target=send_heatbeat, args=(conn,))
    thread.start()

    while True:
        # WORKER_LOGGER.info("[SERVER] %s", worker.recv(2048).decode(FORMAT))
        pass


def send_heatbeat(conn: socket.socket):
    """Send heatbeat to server."""
    while True:
        time.sleep((HEARTBEAT_TIMEOUT_MS / 1000) * 0.5)
        conn.send(b"heartbeat")
