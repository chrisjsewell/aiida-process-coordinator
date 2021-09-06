import asyncio
import logging
import os
from asyncio.streams import StreamReader, StreamWriter
from typing import Dict, NamedTuple, Set

import click
import psutil
from sqlalchemy import func as sql_func
from sqlalchemy import select as sql_select
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session
from sqlalchemy.sql.expression import update

from aiida_task.database import ProcessSchedule, get_session

from .cli.daemon import OPT_LOGLEVEL
from .shared import (
    DATABASE_POLL_MS,
    HANDSHAKE_TIMEOUT,
    MAX_PROCS_PER_WORKER,
    get_socket_from_fd,
    receive_json,
    send_json,
)

SERVER_LOGGER = logging.getLogger(f"server-{os.getuid()}-{os.getpid()}")


@click.command()
@click.argument("network_uuid")
@click.option("--circus-endpoint", help="The circus endpoint")
@click.option(
    "--worker-fd",
    type=int,
    help="socket file descriptor to listen for workers",
)
@click.option(
    "--push-fd",
    type=int,
    help="socket file descriptor to listen for pushes",
)
@click.option("--db-path", help="the aiida database path")
@OPT_LOGLEVEL
def start_coordinator(
    network_uuid: str,
    circus_endpoint: str,
    worker_fd: int,
    push_fd: int,
    db_path: str,
    log_level: str,
):
    """Start the worker coordinator."""
    asyncio.run(
        main(network_uuid, worker_fd, push_fd, circus_endpoint, db_path, log_level)
    )


class WorkerConnection(NamedTuple):
    """Information about a connected worker."""

    pid: int
    uuid: str
    ctime: float
    reader: StreamReader
    writer: StreamWriter


async def main(
    network_uuid: str,
    worker_fd: int,
    push_fd: int,
    circus_endpoint: str,
    db_path: str,
    log_level: str = "INFO",
):
    """Main function."""
    SERVER_LOGGER.setLevel(getattr(logging, log_level.upper()))
    SERVER_LOGGER.info("[STARTING] coordinator is starting...")

    # pid -> connection details
    workers: Dict[int, WorkerConnection] = {}

    # Start the server listening for worker connections
    worker_sock = get_socket_from_fd(worker_fd)
    worker_server = await asyncio.start_server(
        create_worker_connect_cb(network_uuid, workers), sock=worker_sock
    )
    SERVER_LOGGER.info("[LISTENING] for workers on %s", worker_sock.getsockname())

    # Start the server listening for user connections
    push_sock = get_socket_from_fd(push_fd)
    check_db = {True}  # any mutable object will do

    async def push_connect_callback(reader: StreamReader, writer: StreamWriter):
        # check for actual message?
        SERVER_LOGGER.debug("[RECEIVED PUSH]")
        check_db.add(True)
        writer.close()

    push_server = await asyncio.start_server(push_connect_callback, sock=push_sock)
    SERVER_LOGGER.info("[LISTENING] for users on %s", push_sock.getsockname())

    async with worker_server:
        async with push_server:
            # start polling the database
            await coordinate_processes(circus_endpoint, db_path, workers, check_db)

    SERVER_LOGGER.info("[STOPPED]")


def create_worker_connect_cb(network_uuid: str, workers: Dict[int, WorkerConnection]):
    async def _accept_connection(reader: StreamReader, writer: StreamWriter):
        """On a new connection, we perform a handshake with the worker, then store it for later communication"""
        addr = writer.get_extra_info("peername")
        SERVER_LOGGER.info("[NEW CONNECTION] %s", addr)

        data = await receive_json(reader, HANDSHAKE_TIMEOUT)
        assert data["type"] == "handshake"
        nid, pid, uuid = (
            data["data"]["network"],
            data["data"]["pid"],
            data["data"]["uuid"],
        )

        # make sure the worker is also controlled by the same circus daemon
        if network_uuid != nid:
            raise ValueError(
                f"[HANDSHAKE REJECTED] {addr}, network ID {nid} does not match {network_uuid}"
            )
        # make sure a connection of the same PID is not already in use
        if pid in workers:
            raise ValueError(f"[HANDSHAKE REJECTED] {addr}, pid {pid} already in use")

        SERVER_LOGGER.info("[HANDSHAKE COMPLETE] %s PID %s", addr, pid)
        # store the connection details
        workers[pid] = WorkerConnection(
            pid=pid,
            uuid=uuid,
            ctime=psutil.Process(pid).create_time(),
            reader=reader,
            writer=writer,
        )
        SERVER_LOGGER.info(f"[STORED CONNECTIONS] {len(workers)}")

    return _accept_connection


async def coordinate_processes(
    circus_endpoint: str,
    db_path: str,
    workers: Dict[int, WorkerConnection],
    check_db: Set[bool],
):
    """Connect to the databas and handle submitting processes to workers

    - All processed should be submitted (if limits allow)
    - A process should only be running on a single worker
    """
    session = get_session(db_path)

    # Setup queries
    select_processes = sql_select(ProcessSchedule).order_by(ProcessSchedule.mtime.asc())
    select_workloads = (
        sql_select(
            ProcessSchedule.worker_pid.label("worker"),
            sql_func.count(ProcessSchedule.id).label("count"),
        )
        .where(ProcessSchedule.worker_pid.isnot(None))
        .group_by("worker")
    )

    # Record node pks we have sent kill commands for,
    # to avoid logging duplication
    killing: Set[int] = set()

    while True:

        # relinquish control to other tasks
        # If we have received a "push" message, we know there are changes to the database
        # and poll the database immediately
        # if not, we wait for longer, to reduce the load on the cpu/database
        if check_db:
            await asyncio.sleep(0)
            check_db.clear()
        await asyncio.sleep(DATABASE_POLL_MS / 1000)

        # clean up the workers:
        # TODO Here we want to remove any workers that are no longer running from the record
        # so that the processes they were running can be re-assigned.
        # We can easily identify PIDs that are no longer running,
        # more tricky though is identifying any PIDs that have been re-used,
        # (or perhaps workers that are hanging)
        # We could ping the worker and await a response (perhaps containing the processes it is running),
        # but that is problematic if the worker is very busy and takes time to respond
        # (how long should we wait before timing out?)
        # Probably an extra layer here, is to have an additional async task running in the background,
        # which polls the workers with a pretty large timeout, and then removes any workers that do not respond
        for worker_pid in list(workers):
            remove = False
            try:
                proc = psutil.Process(worker_pid)
            except psutil.NoSuchProcess:  # TODO can this fail in any other way?
                remove = True
            else:
                # guard against PID re-use
                if proc.create_time() > workers[worker_pid].ctime:
                    remove = True
            if remove:
                SERVER_LOGGER.info("[REMOVE DEAD WORKER] %s", worker_pid)
                try:
                    workers[worker_pid].writer.close()
                except Exception:
                    pass
                workers.pop(worker_pid)

        # for sqlite we restart the session every loop to deal with write locking,
        # but for postgresql we can probably just have a single session open
        with session:

            # remove old workers from database
            try:
                session.execute(
                    update(ProcessSchedule)
                    .where(ProcessSchedule.worker_pid.notin_(list(workers)))
                    .values(worker_pid=None, worker_uuid=None)
                )
                session.commit()
            except OperationalError:
                # for sqlite only, database could be locked by worker writing
                session.rollback()
                continue

            if not workers:
                # We cannot (re)assign processes until there are workers available
                continue

            # get the number of processes being run by each worker,
            # so we can determine how to assign processes later
            worker_procs = dict(session.execute(select_workloads).all())
            for pid in workers:
                worker_procs.setdefault(pid, 0)

            # iterate through active processes
            process: ProcessSchedule
            for process in session.execute(select_processes).scalars():

                # if the process has already terminated, remove it from active processes
                # TODO is this too slow?
                if process.node.is_terminated:
                    process_id = process.id
                    try:
                        session.delete(process)
                        session.commit()
                    except OperationalError:
                        # for sqlite only, database could be locked by worker writing
                        session.rollback()
                    killing.discard(process_id)
                    continue

                if process.worker_pid is None:
                    # get worker to run process, adding to worker with least processes
                    pid, nprocs = list(
                        sorted(worker_procs.items(), key=lambda x: x[1])
                    )[0]
                    if nprocs < MAX_PROCS_PER_WORKER:
                        if await continue_process(process, session, workers[pid]):
                            worker_procs[pid] += 1

                if process.worker_pid is None:
                    continue

                worker = workers[process.worker_pid]

                # handle actions on process

                if process.action == "kill":
                    # we keep sending this action,
                    # until the process is terminated and removed from the db table (see above)
                    # this obviously assumes that the worker is happy to receive multiple kill requests
                    # for the same process

                    # Don't log multiple times
                    if process.id not in killing:
                        SERVER_LOGGER.info(
                            f"[PROCESS] Killing process {process.id} (node {process.dbnode_id}) "
                            f"on worker {worker.pid}"
                        )
                        killing.add(process.id)

                    try:
                        await send_json(
                            worker.writer,
                            "kill",
                            {
                                "uuid": worker.uuid,
                                "node_id": process.dbnode_id,
                            },
                        )
                    except Exception:
                        SERVER_LOGGER.exception(
                            "[PROCESS] Failed to send kill to worker", worker.pid
                        )

                # TODO pause/play


async def continue_process(
    process: ProcessSchedule, session: Session, worker: WorkerConnection
) -> bool:
    """Send the continue task to the worker"""
    SERVER_LOGGER.info(
        f"[PROCESS] Continuing process {process.id} (node {process.dbnode_id}) "
        f"on worker {worker.pid}"
    )

    # TODO maybe ideally save to database once process is successfully sent to worker
    # but what if database save fails (mainly an sqlite issue)
    try:
        process.worker_pid = worker.pid
        process.worker_uuid = worker.uuid
        session.commit()
    except OperationalError:
        # for sqlite only, database could be locked by worker writing
        session.rollback()
        return False

    try:
        await send_json(
            worker.writer,
            "continue",
            {
                "uuid": worker.uuid,
                "node_id": process.dbnode_id,
            },
        )
    except Exception:
        SERVER_LOGGER.exception(
            "[PROCESS] Failed to send continue to worker", worker.pid
        )
        return False

    return True
