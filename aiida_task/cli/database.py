import click
from sqlalchemy import func, select

from aiida_task.database import Node, RunningProcesses

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
        proc_count = session.scalar(select(func.count(RunningProcesses.id)))
    click.echo(f"{node_count} nodes in the database")
    click.echo(f"{proc_count} running processes in the database")


@database.command("submit")
@pass_db
def submit(db: DatabaseContext):
    """Create and submit a process"""
    with db as session:
        node = Node()
        session.add(node)
        session.commit()
        proc = RunningProcesses(dbnode_id=node.id)
        session.add(proc)
        session.commit()
        click.echo(f"Node {node.id} submitted as process {proc.id}")
