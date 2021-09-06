import asyncio
import logging
import os
import socket
import uuid
from typing import Dict

import click
from sqlalchemy.sql.expression import update

from aiida_task.database import Node, get_session

from .cli.daemon import OPT_LOGLEVEL
from .shared import (
    SOCKET_FAMILY,
    SOCKET_TYPE,
    get_socket_from_fd,
    receive_json,
    send_json,
)

WORKER_LOGGER = logging.getLogger(f"worker-{os.getuid()}-{os.getpid()}")


@click.command()
@click.argument("network_uuid")
@click.option("--socket-fd", type=int, help="the socket file descriptor")
@click.option("--db-path", help="the aiida database path")
@OPT_LOGLEVEL
def start_worker(network_uuid: str, socket_fd: int, db_path: str, log_level: str):
    """Start a worker"""
    asyncio.run(main(network_uuid, socket_fd, db_path, log_level))


async def main(network_uuid: str, socket_fd: int, db_path: str, log_level: str):
    """Main function"""
    WORKER_LOGGER.setLevel(getattr(logging, log_level.upper()))
    WORKER_LOGGER.info("[STARTING] worker is starting...")
    sock = get_socket_from_fd(socket_fd)
    WORKER_LOGGER.info("[CONNECTING] to server: %s", sock.getsockname())
    conn = socket.socket(family=SOCKET_FAMILY, type=SOCKET_TYPE)
    conn.connect(sock.getsockname())
    reader, writer = await asyncio.open_connection(sock=conn)
    worker_uuid = str(uuid.uuid4())
    await send_json(
        writer,
        "handshake",
        {
            "network": network_uuid,
            "uuid": worker_uuid,
            "uid": os.getuid(),
            "pid": os.getpid(),
        },
    )
    WORKER_LOGGER.info("[HANDSHAKE COMPLETE]")

    # node id -> running task
    processes: Dict[str, asyncio.Task] = {}

    while True:
        await asyncio.sleep(0)  # relinquish control to other tasks
        try:
            message = await receive_json(reader, None)
        except Exception:
            WORKER_LOGGER.exception("[MESSAGE ERROR]")
            continue
        msg_type = message.get("type")
        if msg_type == "continue":
            node_id = message["data"]["node_id"]
            if message["data"]["uuid"] == worker_uuid and node_id not in processes:
                # TODO handle if the process is already running or uuid different? (should never happen)
                WORKER_LOGGER.info(f"[CONTINUE] process node {node_id}")
                processes[node_id] = asyncio.create_task(
                    continue_process(db_path, node_id)
                )
        elif msg_type == "kill":
            node_id = message["data"]["node_id"]
            if message["data"]["uuid"] == worker_uuid and node_id in processes:
                # TODO handle if the process is not already running or uuid different? (should never happen)
                task = processes[node_id]
                task.cancel()
        else:
            WORKER_LOGGER.warning("[MESSAGE] Unknown type: %s", msg_type)

    WORKER_LOGGER.info("[HALTING] Closing worker")
    writer.close()


async def continue_process(db_path, node_id):
    """Continue a "mock" process, with handling of cancellation."""
    session = get_session(db_path)
    with session:
        session.execute(update(Node).where(Node.id == node_id).values(status="running"))
        session.commit()
    final_status = "finished"
    try:
        await asyncio.sleep(60)  # mock execution of process for 60 seconds
    except asyncio.CancelledError:
        final_status = "killed"
        WORKER_LOGGER.info(f"[KILLED] process {node_id}")
    else:
        WORKER_LOGGER.info(f"[FINISH] process {node_id}")
    with session:
        session.execute(
            update(Node).where(Node.id == node_id).values(status=final_status)
        )
        session.commit()
