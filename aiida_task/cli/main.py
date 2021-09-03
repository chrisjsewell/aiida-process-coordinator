import os

import click

from aiida_task import __version__
from aiida_task.database import get_session as _get_session


class DatabaseContext:
    """Lazy create the database."""

    def __init__(self, path: str):
        self._path = path

    @property
    def path(self):
        return self._path

    def ensure_exists(self):
        _get_session(self.path)

    def __enter__(self):
        self._session = _get_session(self.path)
        return self._session

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            self._session.rollback()
        else:
            self._session.commit()
        self._session.close()


pass_db = click.make_pass_decorator(DatabaseContext)


@click.group("main")
@click.version_option(__version__)
@click.option(
    "-d",
    "--database",
    default=os.environ.get("AIIDADB", os.path.join(os.getcwd(), "aiida.sqlite3")),
    show_default=True,
    help="Path to database (or set env AIIDADB)",
)
@click.pass_context
def main(
    ctx, database, context_settings={"help_option_names": ("--help",)}  # noqa: B006
):
    ctx.obj = DatabaseContext(database)
