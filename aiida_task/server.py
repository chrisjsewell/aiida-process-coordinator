import logging
import os
import select
import socket
import threading
import time
import uuid
from typing import Dict

import click
from sqlalchemy import func as sql_func
from sqlalchemy import select as sql_select
from sqlalchemy.exc import OperationalError
from sqlalchemy.sql.expression import update

from aiida_task.database import ActiveProcesses, get_session

from .cli.daemon import OPT_LOGLEVEL
from .shared import (
    DATABASE_POLL_MS,
    DISCONNECT_HEADER,
    HEADER_LEN,
    HEARTBEAT_HEADER,
    HEARTBEAT_TIMEOUT_MS,
    MAX_PROCS_PER_WORKER,
    get_socket_from_fd,
    receive_data,
    send_data,
)

# we use an additional uuid to guard against pid re-use
SERVER_ID = (("uid", os.getuid()), ("pid", os.getpid()), ("uuid", str(uuid.uuid4())))
SERVER_LOGGER = logging.getLogger(f"server-{os.getuid()}-{os.getpid()}")

# TODO this is just a quick and dirty method to store active clients across threads
# pid -> {"conn": connection, "uuid": uuid}
ACTIVE_WORKERS: Dict[int, dict] = {}
# TODO also quick and dirty to stop all threads gracefully
# see also https://stackoverflow.com/a/27261365/5033292
STOP_WORKER_THREADS = False


@click.command()
@click.option("--fd", help="the socket file descriptor")
@click.option("--db-path", help="the aiida database path")
@OPT_LOGLEVEL
def start_server(log_level, fd, db_path):
    """Start the server."""
    SERVER_LOGGER.setLevel(getattr(logging, log_level.upper()))
    SERVER_LOGGER.info("[STARTING] server is starting...")

    server = get_socket_from_fd(int(fd))
    server.settimeout(0.2)
    server.listen(5)

    SERVER_LOGGER.info(f"[LISTENING] Server is listening on {server.getsockname()}")

    db_thread = threading.Thread(target=db_connection, args=(db_path,))
    db_thread.start()

    while db_thread.is_alive():
        try:
            conn, addr = server.accept()
        except socket.timeout:
            continue
        SERVER_LOGGER.info(f"[NEW CONNECTION] {addr} connected.")
        worker_thread = threading.Thread(target=worker_connection, args=(conn, addr))
        worker_thread.start()
        # TODO check ACTIVE_WORKERS same as `threading.active_count() - 2`?
        SERVER_LOGGER.info(f"[ACTIVE CONNECTIONS] {threading.active_count() - 2}")

    SERVER_LOGGER.info("[STOPPING] connection to database lost")
    global STOP_WORKER_THREADS
    STOP_WORKER_THREADS = True
    server.close()


class Disconnect(ConnectionResetError):
    pass


def db_connection(db_path):
    """Connects to the database and handles submitting processes to workers.

    - All processed should be submitted (if limits allow)
    - A process should only be running on a single worker
    """
    session = get_session(db_path)
    # look at the oldest process first when iterating over the processes
    select_processes = sql_select(ActiveProcesses).order_by(ActiveProcesses.mtime.asc())
    select_workloads = (
        sql_select(
            ActiveProcesses.worker_pid.label("worker"),
            sql_func.count(ActiveProcesses.id).label("count"),
        )
        .where(ActiveProcesses.worker_pid.isnot(None))
        .group_by("worker")
    )

    while True:
        # for sqlite we restart the session every loop to deal with write locking,
        # but for postgresql we can probably just have a single session open
        with session:
            # SERVER_LOGGER.info(
            #     f"[DB] Initial #processes outstanding: {session.query(ActiveProcesses).count()}"
            # )
            # TODO ensure Node and RunningPRocesses are in-sync
            # (or merge the tables, but maybe this would be too slow)
            # TODO only iterate if ActiveProcesses table or ACTIVE_WORKERS has changed
            if ACTIVE_WORKERS:

                # remove inactive workers
                # we assume here any worker that loses connection will terminate itself,
                # and therefore there will not be duplicate instances of the process running
                try:
                    session.execute(
                        update(ActiveProcesses)
                        .where(ActiveProcesses.worker_pid.notin_(list(ACTIVE_WORKERS)))
                        .values(worker_pid=None, worker_uuid=None)
                    )
                    session.commit()
                except OperationalError:
                    # for sqlite only, database could be locked by worker writing
                    continue

                # get the number of processes being run by each worker,
                # so we can determine how to assign processes
                worker_procs = dict(session.execute(select_workloads).all())
                for pid in ACTIVE_WORKERS:
                    worker_procs.setdefault(pid, 0)

                # iterate through active processes
                process: ActiveProcesses
                for process in session.execute(select_processes).scalars():

                    # if the process has already terminated, remove it from ActiveProcesses
                    # TODO is this too slow?
                    if process.node.is_terminated:
                        try:
                            session.delete(process)
                            session.commit()
                        except OperationalError:
                            # for sqlite only, database could be locked by worker writing
                            pass
                        continue

                    # if the process has not been submitted, submit it
                    if not process.worker_pid:
                        # get worker to run process, adding to worker with least processes
                        pid, nprocs = list(
                            sorted(worker_procs.items(), key=lambda x: x[1])
                        )[0]
                        if nprocs < MAX_PROCS_PER_WORKER:
                            SERVER_LOGGER.info(
                                f"[DB] Submitting process {process.id} to worker PID {pid}"
                            )

                            try:
                                process.worker_pid = pid
                                process.worker_uuid = ACTIVE_WORKERS[pid]["uuid"]
                                worker_procs[pid] += 1
                                session.commit()
                            except OperationalError:
                                # for sqlite only, database could be locked by worker writing
                                continue

                            try:
                                send_data(
                                    ACTIVE_WORKERS[pid]["conn"],
                                    "submit",
                                    {
                                        "uuid": ACTIVE_WORKERS[pid]["uuid"],
                                        "node": process.dbnode_id,
                                    },
                                )
                            except Exception:
                                SERVER_LOGGER.error(
                                    f"[DB] Error submitting process {process.id} to worker PID {pid}",
                                    exc_info=True,
                                )
                            # else:
                            #     process.worker_pid = pid
                            #     process.worker_uuid = ACTIVE_WORKERS[pid]["uuid"]
                            #     worker_procs[pid] += 1
                            #     session.commit()

                # TODO handle actions

            time.sleep(DATABASE_POLL_MS / 1000)


def worker_connection(conn: socket.socket, addr):
    """Receive heartbeat from worker, or close connection."""
    uid = pid = uuid = None
    try:
        # confirm connection
        # TODO improve this
        send_data(conn, "connection", dict(SERVER_ID))

        # get worker identifier
        message = receive_data(conn)

        try:
            data = message["data"]
            uid, pid, uuid = (data["uid"], data["pid"], data["uuid"])
        except KeyError:
            raise Disconnect(f"Client identifier not of expected form: {message}")
        this_user = os.getuid()
        if uid != this_user:
            raise Disconnect(
                f"Client not owned by the same user ({this_user}): {message}"
            )
        ACTIVE_WORKERS[pid] = {"uuid": uuid, "conn": conn}
        SERVER_LOGGER.info("[%s] process PID %s", addr, pid)

        poller = select.poll()
        poller.register(conn, select.POLLIN)

        while True:

            if STOP_WORKER_THREADS:
                raise Disconnect("stopping thread")

            # check for heartbeat
            events = poller.poll(HEARTBEAT_TIMEOUT_MS)
            if not events:
                raise Disconnect("Missed heartbeat")
            header = conn.recv(HEADER_LEN)
            if header == HEARTBEAT_HEADER:
                SERVER_LOGGER.debug(f"[HEARTBEAT] {addr}")
            elif header == DISCONNECT_HEADER:
                raise Disconnect("Worker disconnected")

    except ConnectionResetError as exc:
        SERVER_LOGGER.info(f"[DISCONNECTED] {addr}: {exc}")
    finally:
        ACTIVE_WORKERS.pop(pid, None)
        conn.close()
