import json
import logging
import os
import socket
import threading
import time
import uuid
from typing import Any, Dict

import click

from .cli import OPT_LOGLEVEL
from .shared import (
    ENCODING,
    HEADER_LEN,
    HEARTBEAT_HEADER,
    HEARTBEAT_TIMEOUT_MS,
    SOCKET_FAMILY,
    SOCKET_TYPE,
    get_socket_from_fd,
)

WORKER_ID = f"{os.getuid()}-{os.getpid()}"
WORKER_LOGGER = logging.getLogger(f"worker-{WORKER_ID}")


def send_message(conn: socket.socket, data: Dict[str, Any]):
    """Send a message to the server"""
    message = json.dumps(data).encode(ENCODING)
    # send the header, with the message length
    conn.send(len(message).to_bytes(HEADER_LEN, "big"))
    # send the message
    conn.send(message)


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

    # Send worker details to the server
    worker_uuid = str(uuid.uuid4())
    send_message(conn, {"uuid": worker_uuid, "uid": os.getuid(), "pid": os.getpid()})

    # start thread for sending heartbeat
    thread = threading.Thread(target=send_heatbeat, args=(conn,))
    thread.start()

    # stop the process if the heartbeat thread dies
    while thread.is_alive():
        pass

    WORKER_LOGGER.info("Lost connection to server, halting!")


def send_heatbeat(conn: socket.socket):
    """Send heatbeat to server."""
    while True:
        time.sleep((HEARTBEAT_TIMEOUT_MS / 1000) * 0.5)
        # this should except if the server is no longer available
        # may be also receive from server?
        try:
            conn.send(HEARTBEAT_HEADER)
        except BrokenPipeError:
            break
