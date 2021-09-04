import asyncio
import logging
import os
import select
import socket
import threading
import time
import uuid
from typing import List

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
    thread = threading.Thread(target=send_heatbeat, args=(conn,), daemon=True)
    thread.start()

    asyncio.run(watch_for_messages(conn, thread, db_path))

    WORKER_LOGGER.info("[HALTING] Closing worker")


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


async def watch_for_messages(
    conn: socket.socket, thread: threading.Thread, db_path: str
):
    """Watch for messages from the server."""
    processes: List[asyncio.Task] = []
    poller = select.poll()
    poller.register(conn, select.POLLIN)

    while thread.is_alive():
        await asyncio.sleep(0)  # relinquish control to other tasks
        events = poller.poll(HEARTBEAT_TIMEOUT_MS)
        if not events:
            continue
        try:
            message = receive_data(conn)
        except ConnectionResetError:
            break
        if not message:
            continue
        if message.get("type") == "submit":
            node_id = message["data"]["node"]
            processes.append(asyncio.create_task(run_process(db_path, node_id)))

    WORKER_LOGGER.info("[HALTING] Lost server connection, cancelling processes")

    for process in processes:
        process.cancel()
    # TODO wait for them to finalise cancellation


async def run_process(db_path, node_id):
    """Run a process to termination."""
    WORKER_LOGGER.info(f"[SUBMIT] process {node_id}")
    await asyncio.sleep(60)
    session = get_session(db_path)
    with session:
        session.execute(
            update(Node).where(Node.id == node_id).values(status="finished")
        )
        session.commit()
    WORKER_LOGGER.info(f"[FINISH] process {node_id}")
