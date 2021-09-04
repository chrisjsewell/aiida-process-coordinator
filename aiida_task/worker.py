import logging
import os
import select
import socket
import threading
import time
import uuid

import click
from sqlalchemy.sql.expression import update

from aiida_task.database import Node, get_session

from .cli.daemon import OPT_LOGLEVEL
from .shared import (
    HEARTBEAT_HEADER,
    HEARTBEAT_TIMEOUT_MS,
    SOCKET_FAMILY,
    SOCKET_TYPE,
    get_socket_from_fd,
    receive_data,
    send_data,
)

# we use an additional uuid to guard against pid re-use
WORKER_ID = (("uid", os.getuid()), ("pid", os.getpid()), ("uuid", str(uuid.uuid4())))
WORKER_LOGGER = logging.getLogger(f"worker-{os.getuid()}-{os.getpid()}")


@click.command()
@click.option("--fd", help="the socket file descriptor")
@click.option("--db-path", help="the aiida database path")
@OPT_LOGLEVEL
def start_worker(log_level, fd, db_path):
    """Start a worker"""
    WORKER_LOGGER.setLevel(getattr(logging, log_level.upper()))
    WORKER_LOGGER.info("[STARTING] worker is starting...")

    sock = get_socket_from_fd(int(fd))
    conn = socket.socket(family=SOCKET_FAMILY, type=SOCKET_TYPE)
    conn.connect(sock.getsockname())
    WORKER_LOGGER.info("[CONNECTED] to server: %s", sock.getsockname())

    # Send worker details to the server
    send_data(conn, "connection", dict(WORKER_ID))

    # start thread for sending heartbeat
    thread = threading.Thread(target=send_heatbeat, args=(conn,))
    thread.start()

    poller = select.poll()
    poller.register(conn, select.POLLIN)

    # stop the process if the heartbeat thread dies
    while thread.is_alive():
        events = poller.poll(HEARTBEAT_TIMEOUT_MS)
        if not events:
            continue
        # expect to receive a message from the server to start a new task or action
        try:
            message = receive_data(conn)
        except ConnectionResetError:
            break
        if not message:
            continue
        if message.get("type") == "submit":
            node_id = message["data"]["node"]
            WORKER_LOGGER.info(f"[SUBMIT] process {node_id}")
            time.sleep(10)
            session = get_session(db_path)
            with session:
                session.execute(
                    update(Node).where(Node.id == node_id).values(status="finished")
                )
                session.commit()
            WORKER_LOGGER.info(f"[FINISH] process {node_id}")

    WORKER_LOGGER.info("[HALTING] Lost connection to server")


def send_heatbeat(conn: socket.socket):
    """Send heatbeat to server."""
    while True:
        time.sleep((HEARTBEAT_TIMEOUT_MS / 1000) * 0.5)
        # this should except if the server is no longer available
        # TODO maybe also receive from server?
        try:
            conn.send(HEARTBEAT_HEADER)
        except BrokenPipeError:
            break
