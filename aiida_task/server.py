import logging
import os
import select
import socket
import threading

import click

from .cli import OPT_LOGLEVEL
from .shared import (
    DISCONNECT_MESSAGE,
    FORMAT,
    HEADER,
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
        thread = threading.Thread(target=worker_heartbeat, args=(conn, addr))
        thread.start()
        SERVER_LOGGER.info(f"[ACTIVE CONNECTIONS] {threading.active_count() - 1}")


class Disconnected(ConnectionResetError):
    pass


def worker_heartbeat(conn: socket.socket, addr):
    """Receive heartbeat from worker, or close connection."""
    try:

        conn.send("You are connected".encode(FORMAT))
        poller = select.poll()
        poller.register(conn, select.POLLIN)

        while True:

            # get heartbeat
            events = poller.poll(HEARTBEAT_TIMEOUT_MS)
            if not events:
                raise Disconnected("Missed heartbeat")

            msg_length = conn.recv(HEADER).decode(FORMAT)
            if msg_length == "heartbeat":
                SERVER_LOGGER.debug(f"[HEARTBEAT] {addr}.")
                continue

            if msg_length:
                msg_length = int(msg_length)
                msg = conn.recv(msg_length).decode(FORMAT)
                if msg == DISCONNECT_MESSAGE:
                    raise Disconnected("Worker disconnected")

                SERVER_LOGGER.info(f"[{addr}] {msg}")
                conn.send("Msg received".encode(FORMAT))

    except ConnectionResetError as exc:
        SERVER_LOGGER.info(f"[DISCONNECTED] {addr}: {exc}")
    finally:
        conn.close()
