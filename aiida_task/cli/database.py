import click
import psutil
import yaml
from sqlalchemy import func, select
from sqlalchemy.exc import OperationalError

from aiida_task.database import TERMINATED_STATES, ActiveProcesses, Node
from aiida_task.shared import MAX_PROCS_PER_WORKER

from .main import DatabaseContext, main, pass_db


@main.group("database")
def database():
    """Manage the database"""


@database.command("status")
@pass_db
def status(db: DatabaseContext):
    """Show the status of the database"""
    with db as session:
        node_count = session.scalar(select(func.count(Node.id)))
        proc_count = session.scalar(select(func.count(ActiveProcesses.id)))
        unterminated_count = session.scalar(
            select(func.count(Node.id)).where(Node.status.notin_(TERMINATED_STATES))
        )
        worker_proc_count = session.execute(
            select(
                ActiveProcesses.worker_pid.label("worker"),
                func.count(ActiveProcesses.id).label("count"),
            )
            .where(ActiveProcesses.worker_pid.isnot(None))
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


@database.command("submit")
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
                proc = ActiveProcesses(dbnode_id=node.id)
                session.add(proc)
                session.commit()
                click.echo(f"Node {node.id} submitted as process {proc.id}")
            except OperationalError:
                click.echo("Database locked, skipping!")
                session.rollback()
