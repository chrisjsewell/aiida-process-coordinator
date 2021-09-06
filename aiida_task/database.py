"""Mock AiiDA database"""
import os
from datetime import datetime as timezone

from sqlalchemy import (
    Column,
    DateTime,
    ForeignKey,
    Integer,
    String,
    create_engine,
    event,
)
from sqlalchemy.orm import declarative_base, relationship, sessionmaker
from sqlalchemy.orm.session import Session
from sqlalchemy.sql.expression import text

Base = declarative_base()

TERMINATED_STATES = ("finished", "excepted", "killed")


class Node(Base):
    """Mock implementation of the node."""

    __tablename__ = "db_dbnode"

    id = Column(Integer, primary_key=True)
    mtime = Column(DateTime(timezone=True), default=timezone.now, onupdate=timezone.now)
    status = Column(String(36), default="created", nullable=False)

    @property
    def is_terminated(self):
        return self.status in TERMINATED_STATES


class ProcessSchedule(Base):
    """A new table, which stores information about running processes."""

    __tablename__ = "db_dbprocess"

    id = Column(Integer, primary_key=True)
    mtime = Column(DateTime(timezone=True), default=timezone.now, onupdate=timezone.now)
    # TODO currently the process record is deleted when the process node is deleted
    # but if the process is already being run by a worker,
    # it will continue until it excepts, because it cannot update the node
    # alternatively, we could maybe set `ondelete="SET NULL"`
    # and have the server handle killing the process, before removing the record
    dbnode_id = Column(
        Integer,
        ForeignKey("db_dbnode.id", ondelete="CASCADE"),
        nullable=False,
        unique=True,
    )
    node = relationship("Node")
    # an action that has been requested for this process: pause | play | kill
    action = Column(String(255), nullable=True)
    # the identifiers for the worker running the process (if assigned)
    # we use an additional uuid, generated by the worker, to guard against pid re-use
    worker_pid = Column(Integer, nullable=True)
    worker_uuid = Column(String(36), nullable=True)


def get_session(path: str) -> Session:
    """Return a new session to connect to the SQLite DB (created if missing)."""
    create = not os.path.exists(path)

    engine = create_engine(f"sqlite:///{path}", future=True)

    # For the next two bindings, see background on
    # https://docs.sqlalchemy.org/en/13/dialects/sqlite.html#serializable-isolation-savepoints-transactional-ddl
    @event.listens_for(engine, "connect")
    def do_connect(dbapi_connection, _):
        """Hook function that is called upon connection.

        It modifies the default behavior of SQLite to use WAL and to
        go back to the 'default' isolation level mode.
        """
        # disable pysqlite's emitting of the BEGIN statement entirely.
        # also stops it from emitting COMMIT before any DDL.
        dbapi_connection.isolation_level = None
        # Open the file in WAL mode (see e.g. https://stackoverflow.com/questions/9671490)
        # This allows to have as many readers as one wants, and a concurrent writer (up to one)
        # Note that this writes on a journal, on a different packs.idx-wal,
        # and also creates a packs.idx-shm file.
        # Note also that when the session is created, you will keep reading from the same version,
        # so you need to close and reload the session to see the newly written data.
        # Docs on WAL: https://www.sqlite.org/wal.html
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA journal_mode=wal;")
        cursor.close()

    # For this binding, see background on
    # https://docs.sqlalchemy.org/en/13/dialects/sqlite.html#serializable-isolation-savepoints-transactional-ddl
    @event.listens_for(engine, "begin")
    def do_begin(conn):  # pylint: disable=unused-variable
        # emit our own BEGIN
        conn.execute(text("BEGIN"))

    if create:
        # Create all tables in the engine. This is equivalent to "Create Table"
        # statements in raw SQL.
        Base.metadata.create_all(engine)

    # Bind the engine to the metadata of the Base class so that the
    # declaratives can be accessed through a DBSession instance
    Base.metadata.bind = engine

    # We set autoflush = False to avoid to lock the DB if just doing queries/reads
    session = sessionmaker(
        bind=engine, autoflush=False, autocommit=False, future=True
    )()

    return session
