import json
import logging
import os
import select
import socket
import threading
from typing import Any, Dict, Optional

import click

from .cli import OPT_LOGLEVEL
from .shared import (
    DISCONNECT_HEADER,
    ENCODING,
    HEADER_LEN,
    HEARTBEAT_HEADER,
    HEARTBEAT_TIMEOUT_MS,
    get_socket_from_fd,
)

SERVER_ID = f"{os.getuid()}-{os.getpid()}"
SERVER_LOGGER = logging.getLogger(f"server-{SERVER_ID}")


@click.command()
@click.option("--fd")
@OPT_LOGLEVEL
def start_server(log_level, fd):
    """Start the server."""
    SERVER_LOGGER.setLevel(getattr(logging, log_level.upper()))
    SERVER_LOGGER.info("[STARTING] server is starting...")

    server = get_socket_from_fd(int(fd))
    server.listen(5)

    SERVER_LOGGER.info(f"[LISTENING] Server is listening on {server.getsockname()}")

    while True:
        conn, addr = server.accept()
        SERVER_LOGGER.info(f"[NEW CONNECTION] {addr} connected.")
        thread = threading.Thread(target=worker_connection, args=(conn, addr))
        thread.start()
        SERVER_LOGGER.info(f"[ACTIVE CONNECTIONS] {threading.active_count() - 1}")


class Disconnected(ConnectionResetError):
    pass


def receive_message(conn: socket.socket, addr) -> Optional[Dict[str, Any]]:
    """Wait for a message from the client."""
    # first expect a heading with a heartbeat or the length of the coming message
    header = conn.recv(HEADER_LEN)
    if not header:
        return
    if header == HEARTBEAT_HEADER:
        SERVER_LOGGER.debug(f"[HEARTBEAT] {addr}")
        return
    if header == DISCONNECT_HEADER:
        raise Disconnected("Worker disconnected")
    msg_length = int.from_bytes(header, "big")
    # now expect the message
    message = conn.recv(msg_length)
    # the message should be an encoded JSON dict
    return json.loads(message.decode(ENCODING))


def worker_connection(conn: socket.socket, addr):
    """Receive heartbeat from worker, or close connection."""
    try:

        # confirm connection
        conn.send("You are connected".encode(ENCODING))

        # get worker details
        message = receive_message(conn, addr)
        SERVER_LOGGER.info("[%s] %s", addr, message)

        poller = select.poll()
        poller.register(conn, select.POLLIN)

        while True:

            # check for heartbeat
            events = poller.poll(HEARTBEAT_TIMEOUT_MS)
            if not events:
                raise Disconnected("Missed heartbeat")

            message = receive_message(conn, addr)

            #  SERVER_LOGGER.info(f"[{addr}] {msg}")
            #  conn.send("Msg received".encode(ENCODING))

    except ConnectionResetError as exc:
        SERVER_LOGGER.info(f"[DISCONNECTED] {addr}: {exc}")
    finally:
        conn.close()
