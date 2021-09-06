import os

import click
import psutil
import tabulate
import yaml
from sqlalchemy import desc, func, select, update
from sqlalchemy.exc import OperationalError

from aiida_task.database import TERMINATED_STATES, Node, ProcessSchedule
from aiida_task.shared import MAX_PROCS_PER_WORKER

from .main import DatabaseContext, main, pass_db


@main.group("process")
def process():
    """Manage processes"""


@process.command("remove-db")
@pass_db
def remove(db: DatabaseContext):
    """Clear the database"""
    if os.path.exists(db.path):
        click.confirm(
            f"Are you sure you want to remove the database at {db.path}?",
            abort=True,
        )
        os.remove(db.path)
        if os.path.exists(db.path + "-shm"):
            os.remove(db.path + "-shm")
        if os.path.exists(db.path + "-wal"):
            os.remove(db.path + "-wal")
        click.echo("Database removed")


@process.command("status")
@pass_db
def status(db: DatabaseContext):
    """Show the status of the processes in the database"""
    with db as session:
        node_count = session.scalar(select(func.count(Node.id)))
        proc_count = session.scalar(select(func.count(ProcessSchedule.id)))
        unterminated_count = session.scalar(
            select(func.count(Node.id)).where(Node.status.notin_(TERMINATED_STATES))
        )
        worker_proc_count = session.execute(
            select(
                ProcessSchedule.worker_pid.label("worker"),
                func.count(ProcessSchedule.id).label("count"),
            )
            .where(ProcessSchedule.worker_pid.isnot(None))
            .group_by("worker")
            .order_by("count")
        ).all()

    def _pid_exists(pid):
        try:
            return psutil.pid_exists(pid)
        except Exception:
            return False

    data = {
        "Process nodes": node_count,
        "Non-terminated nodes": unterminated_count,
        "Active processes": proc_count,
        "Worker loads (PID -> count/max)": {
            key: f"{val} / {MAX_PROCS_PER_WORKER}"
            for key, val in worker_proc_count
            if _pid_exists(key)
        },
    }
    click.echo(yaml.dump(data, sort_keys=False))


@process.command("list")
@click.option("-l", "--last", type=int, default=10, show_default=True)
@pass_db
def process_list(db: DatabaseContext, last: int):
    """List the processes in the database"""
    with db as session:
        result = session.execute(
            select(Node.id, Node.mtime, Node.status)
            .order_by(desc(Node.mtime))
            .limit(last)
        ).all()

        click.echo(tabulate.tabulate(result, headers=["PK", "Modified", "Status"]))


@process.command("scheduled")
@click.option("-l", "--last", type=int, default=10, show_default=True)
@pass_db
def process_list_active(db: DatabaseContext, last: int):
    """List the process scheduled to run in the database"""
    with db as session:
        result = session.execute(
            select(
                ProcessSchedule.dbnode_id,
                ProcessSchedule.mtime,
                ProcessSchedule.action,
                ProcessSchedule.worker_pid,
            )
            .order_by(desc(ProcessSchedule.mtime))
            .limit(last)
        ).all()
        click.echo(
            tabulate.tabulate(
                result, headers=["PK", "Modified", "Action", "Worker PID"]
            )
        )


@process.command("submit")
@click.argument("number", type=int)
@pass_db
def submit(db: DatabaseContext, number: int):
    """Create and submit a process"""
    with db as session:
        for _ in range(number):
            try:
                node = Node()
                session.add(node)
                session.flush()
                proc = ProcessSchedule(dbnode_id=node.id)
                session.add(proc)
                session.commit()
                click.echo(f"Node pk {node.id} submitted")
            except OperationalError:
                click.echo("Database locked, skipping!")
                session.rollback()


@process.command("kill")
@click.argument("pk", type=int)
@pass_db
def kill(db: DatabaseContext, pk: int):
    """Kill a process node"""
    with db as session:
        if not session.execute(
            select(ProcessSchedule.dbnode_id).where(ProcessSchedule.dbnode_id == pk)
        ).one_or_none():
            click.echo(f"No active process node with pk {pk} found")
            return
        session.execute(
            update(ProcessSchedule)
            .where(ProcessSchedule.dbnode_id == pk)
            .values(action="kill")
        )
        session.commit()
    click.echo(f"Scheduled node pk {pk} to be killed")
