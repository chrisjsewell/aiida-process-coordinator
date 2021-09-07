import asyncio
import logging
import os
from asyncio.streams import StreamReader, StreamWriter
from asyncio.tasks import Task
from dataclasses import dataclass
from typing import Dict, Optional, Set

import click
import psutil
from sqlalchemy import func as sql_func
from sqlalchemy import select as sql_select
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session
from sqlalchemy.sql.expression import delete, update

from aiida_task.database import Node, ProcessSchedule, get_session

from .cli.daemon import OPT_LOGLEVEL
from .shared import (
    DB_POLL_SLEEP_INTERVAL_MS,
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


@dataclass
class WorkerConnection:
    """Information about a connected worker."""

    pid: int
    uuid: str
    ctime: float
    reader: StreamReader
    writer: StreamWriter
    dead: bool = False


@dataclass
class PollSleep:
    """A poll sleep task."""

    sleep: bool
    task: Optional[Task]


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
    poll_sleep = PollSleep(sleep=False, task=None)

    def _cancel_poll_sleep():
        if poll_sleep.task is not None:
            # the polling is sleeping, cancel it
            poll_sleep.task.cancel()
            poll_sleep.task = None
        else:
            # the polling is not sleeping, make sure to re-poll immediately
            poll_sleep.sleep = False

    # Start the server listening for worker connections
    worker_sock = get_socket_from_fd(worker_fd)
    worker_server = await asyncio.start_server(
        create_worker_connect_cb(network_uuid, workers, _cancel_poll_sleep),
        sock=worker_sock,
    )
    SERVER_LOGGER.info("[LISTENING] for workers on %s", worker_sock.getsockname())

    # Start the server listening for user connections
    push_sock = get_socket_from_fd(push_fd)

    async def push_connect_callback(reader: StreamReader, writer: StreamWriter):
        # check for actual message?
        SERVER_LOGGER.debug("[RECEIVED PUSH]")
        # TODO this may need throttling, if e.g. lots of pushes are received at the same time
        _cancel_poll_sleep()
        writer.close()

    push_server = await asyncio.start_server(push_connect_callback, sock=push_sock)
    SERVER_LOGGER.info("[LISTENING] for users on %s", push_sock.getsockname())

    async with worker_server:
        async with push_server:
            task = asyncio.create_task(detect_dead_workers(workers, _cancel_poll_sleep))
            try:
                # start polling the database
                await coordinate_processes(
                    circus_endpoint, db_path, workers, poll_sleep
                )
            finally:
                task.cancel()

    SERVER_LOGGER.info("[STOPPED]")


def create_worker_connect_cb(
    network_uuid: str, workers: Dict[int, WorkerConnection], cancel_poll_sleep
):
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

        cancel_poll_sleep()

    return _accept_connection


async def detect_dead_workers(workers: Dict[int, WorkerConnection], cancel_poll_sleep):
    """Mark dead workers."""

    # Here we want to mark any workers that are no longer running.
    # so that the processes they were running can be re-assigned.
    # We do not actually remove it here,
    # to avoid changing the dictionary size in coordinate_processes.

    # TODO We can easily identify PIDs that are no longer running,
    # more tricky though is identifying any workers that are unresponsive,
    # We could ping the worker and await a response (perhaps containing the processes it is running),
    # but that is problematic if the worker is very busy and takes time to respond
    # (how long should we wait before timing out?)

    while True:

        await asyncio.sleep(0.1)

        for worker in workers.values():
            if worker.dead:
                continue
            dead = False
            try:
                proc = psutil.Process(worker.pid)
            except psutil.NoSuchProcess:
                dead = True
            except Exception:  # TODO can this fail in any other way?
                pass
            else:
                # guard against PID re-use
                if proc.create_time() > worker.ctime:
                    dead = True
            if dead:
                SERVER_LOGGER.info("[REMOVE DEAD WORKER] %s", worker.pid)
                try:
                    worker.writer.close()
                except Exception:
                    pass
                worker.dead = True
                cancel_poll_sleep()


async def coordinate_processes(
    circus_endpoint: str,
    db_path: str,
    workers: Dict[int, WorkerConnection],
    poll_sleep: PollSleep,
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

    # for sqlite we restart the session every loop to deal with write locking,
    # but for postgresql we can probably just have a single session open
    with session:

        while True:

            # relinquish control to other tasks and sleep until the next poll interval
            # If a change event has occurred
            # (receive a "push" message, or a worker has been added/removed),
            # we know we need to poll the database immediately (no sleep)
            # if not, we sleep for longer, to reduce the load on the cpu/database,
            # but cancel this sleep, on a change event
            if poll_sleep.sleep:
                SERVER_LOGGER.debug("[SLEEPING]")
                task = asyncio.create_task(
                    asyncio.sleep(DB_POLL_SLEEP_INTERVAL_MS / 1000)
                )
                poll_sleep.task = task
                try:
                    await task
                except asyncio.CancelledError:
                    SERVER_LOGGER.debug("[SLEEP CANCELLED]")
                else:
                    SERVER_LOGGER.debug("[SLEEP FINISHED]")
                poll_sleep.task = None
            else:
                SERVER_LOGGER.debug("[NOT SLEEPING]")
                await asyncio.sleep(1)
            poll_sleep.sleep = True

            # remove dead workers
            for worker_pid, worker in list(workers.items()):
                if worker.dead:
                    workers.pop(worker_pid)

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
                poll_sleep.sleep = False
                continue

            if not workers:
                # We cannot (re)assign processes until there are workers available
                continue

            # get the number of processes being run by each worker,
            # so we can determine how to assign processes later
            worker_procs = dict(session.execute(select_workloads).all())
            for pid in workers:
                worker_procs.setdefault(pid, 0)

            # delete terminated processes from schedule
            try:
                stmt = (
                    sql_select(ProcessSchedule.id)
                    .join(Node)
                    .where(Node.status.in_(("finished", "excepted", "killed")))
                )
                to_delete = session.execute(stmt).scalars().all()
                if to_delete:
                    session.execute(
                        delete(ProcessSchedule).where(ProcessSchedule.id.in_(to_delete))
                    )
                    session.commit()
                    SERVER_LOGGER.info(
                        "[PROCESSES] removed from schedule: %s", to_delete
                    )
                    killing.difference_update(to_delete)
            except OperationalError:
                # for sqlite only, database could be locked by worker writing
                session.rollback()
                poll_sleep.sleep = False
                continue

            # iterate through active processes
            process: ProcessSchedule
            for process in session.execute(select_processes).scalars():

                if process.worker_pid is None:
                    # get worker to run process, adding to worker with least processes
                    pid, nprocs = list(
                        sorted(worker_procs.items(), key=lambda x: x[1])
                    )[0]
                    if nprocs < MAX_PROCS_PER_WORKER:
                        if await continue_process(process, session, workers[pid]):
                            worker_procs[pid] += 1
                    else:
                        poll_sleep.sleep = False

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
                            "[PROCESS] Failed to send kill to worker %s", worker.pid
                        )

                # TODO pause/play


async def continue_process(
    process: ProcessSchedule, session: Session, worker: WorkerConnection
) -> bool:
    """Send the continue task to the worker"""

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

    SERVER_LOGGER.info(
        f"[PROCESS] Continuing process {process.id} (node {process.dbnode_id}) "
        f"on worker {worker.pid}"
    )

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
