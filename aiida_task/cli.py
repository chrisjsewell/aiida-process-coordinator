import os
import shutil
import socket
import sys
from typing import Optional

import click
import yaml
from circus import get_arbiter
from circus import logger as circus_logger
from circus.circusd import daemonize
from circus.client import CircusClient
from circus.pidfile import Pidfile
from circus.sockets import CircusSocket
from circus.util import (
    DEFAULT_ENDPOINT_DEALER,
    DEFAULT_ENDPOINT_STATS,
    DEFAULT_ENDPOINT_SUB,
    check_future_exception_and_log,
    configure_logger,
)
from click import argument, option

from . import __version__
from .shared import SOCKET_FAMILY, SOCKET_TYPE

CIRCUS_PID_FILE = "circus.pid"
CIRCUS_LOG_FILE = "circus.log"
WATCHER_WORKER_NAME = "aiida-workers"
WATCHER_SERVER_NAME = "aiida-server"


@click.group("main")
@click.version_option(__version__)
def main(context_settings={"help_option_names": ("--help",)}):  # noqa: B006
    pass


def get_env():
    currenv = os.environ.copy()
    currenv["PATH"] = f"{os.path.dirname(sys.executable)}:{currenv['PATH']}"
    currenv["PYTHONUNBUFFERED"] = "True"
    return currenv


def get_circus_client(
    *, endpoint=DEFAULT_ENDPOINT_DEALER, timeout: int = 5, workdir: Optional[str] = None
) -> CircusClient:
    if workdir:
        pidfile = os.path.join(workdir, CIRCUS_PID_FILE)
        if not os.path.exists(pidfile):
            raise SystemExit(f"PID does not exist: {pidfile}")
    return CircusClient(endpoint=endpoint, timeout=timeout)


OPT_WORKDIR = option(
    "--workdir",
    default=os.path.join(os.getcwd(), "workdir"),
    show_default=True,
    help="Directory to store files",
)
OPT_LOGLEVEL = option(
    "-l",
    "--log-level",
    default="info",
    show_default=True,
    type=click.Choice(("debug", "info", "warning", "critical"), case_sensitive=False),
    help="The log level",
)


@main.command("start")
@argument("number", required=False, type=int, default=1)
@OPT_WORKDIR
@OPT_LOGLEVEL
@option("--foreground", is_flag=True, help="Run in foreground")
def circus_start(number, workdir, log_level, foreground):
    """Start daemon"""
    if foreground and number > 1:
        raise click.ClickException(
            "can only run a single worker when running in the foreground"
        )

    worker_name = WATCHER_WORKER_NAME
    server_name = WATCHER_SERVER_NAME

    # set physical file locations
    workdir = os.path.abspath(workdir)
    os.makedirs(workdir, exist_ok=True)
    pidfile = os.path.join(workdir, CIRCUS_PID_FILE)
    logfile_circus = os.path.join(workdir, CIRCUS_LOG_FILE)
    logfile_worker = os.path.join(workdir, f"watcher-{worker_name}.log")
    logfile_server = os.path.join(workdir, f"watcher-{server_name}.log")

    if os.path.exists(pidfile):
        raise click.ClickException(f"PID file already exists: {pidfile}")

    # see circus.arbiter.Arbiter for inputs
    arbiter_config = {
        "controller": DEFAULT_ENDPOINT_DEALER,
        "pubsub_endpoint": DEFAULT_ENDPOINT_SUB,
        "stats_endpoint": DEFAULT_ENDPOINT_STATS,
        "logoutput": "-" if foreground else logfile_circus,
        "loglevel": log_level.upper(),
        "debug": False,
        "statsd": True,
        "pidfile": pidfile,
        # see circus.watchers.Watcher for inputs
        "watchers": [
            {
                "cmd": (
                    f"aiida-server --log-level {log_level}"
                    " --fd $(circus.sockets.messaging)"
                ),
                "name": server_name,
                "singleton": True,
                "virtualenv": os.environ.get("VIRTUAL_ENV", None),
                "copy_env": True,
                "env": get_env(),
                "use_sockets": True,
                "stdout_stream": {
                    "class": "FileStream",
                    "filename": logfile_server,
                },
                "stderr_stream": {
                    "class": "FileStream",
                    "filename": logfile_server,
                },
            },
            {
                "cmd": (
                    f"aiida-worker --log-level {log_level}"
                    " --fd $(circus.sockets.messaging)"
                ),
                "name": worker_name,
                "numprocesses": number,
                "virtualenv": os.environ.get("VIRTUAL_ENV", None),
                "copy_env": True,
                "env": get_env(),
                "use_sockets": True,
                "stdout_stream": {
                    "class": "FileStream",
                    "filename": logfile_worker,
                },
                "stderr_stream": {
                    "class": "FileStream",
                    "filename": logfile_worker,
                },
            },
        ],
    }

    if not foreground:
        daemonize()

    # important: sockets have to be created, after daemonizing
    arbiter_config["sockets"] = [
        CircusSocket(
            name="messaging",
            host=socket.gethostbyname(socket.gethostname()),
            port=0,  # pick an available port
            family=SOCKET_FAMILY,
            type=SOCKET_TYPE,
            blocking=False,
        )
    ]

    arbiter = get_arbiter(**arbiter_config)
    pidfile = Pidfile(arbiter.pidfile)

    try:
        pidfile.create(os.getpid())
    except RuntimeError as exception:
        raise SystemExit(1) from exception

    # Configure the logger
    configure_logger(
        circus_logger,
        level=log_level.upper(),
        output=logfile_circus,
        loggerconfig=arbiter.loggerconfig or None,
    )

    # Main loop
    should_restart = True

    while should_restart:
        try:
            future = arbiter.start()
            should_restart = False
            if check_future_exception_and_log(future) is None:
                should_restart = arbiter._restarting
        except Exception as exception:
            # Emergency stop
            arbiter.loop.run_sync(arbiter._emergency_stop)
            raise exception
        except KeyboardInterrupt:
            pass
        finally:
            arbiter = None
            if pidfile is not None:
                pidfile.unlink()


@main.command("stop")
@OPT_WORKDIR
@option("--clear", is_flag=True, help="Clear the workdir")
def circus_stop(workdir, clear):
    """Stop daemon"""
    client = get_circus_client(workdir=workdir)
    result = client.call({"command": "quit", "properties": {"waiting": True}})
    click.echo(yaml.dump(result))
    if clear:
        shutil.rmtree(workdir)


@main.command("status")
@argument("watcher", default=WATCHER_WORKER_NAME)
@OPT_WORKDIR
def circus_status(watcher, workdir):
    """Daemon status"""
    client = get_circus_client(workdir=workdir)
    result = client.call({"command": "stats", "properties": {"name": watcher}})
    click.echo(yaml.dump(result))


@main.command("incr")
@argument("number", required=False, type=int, default=1)
@OPT_WORKDIR
def circus_incr(number, workdir):
    """Increase workers"""
    client = get_circus_client(workdir=workdir)
    result = client.call(
        {"command": "incr", "properties": {"name": WATCHER_WORKER_NAME, "nb": number}}
    )
    click.echo(yaml.dump(result))


@main.command("decr")
@argument("number", required=False, type=int, default=1)
@OPT_WORKDIR
def circus_decr(number, workdir):
    """Increase workers"""
    client = get_circus_client(workdir=workdir)
    result = client.call(
        {"command": "decr", "properties": {"name": WATCHER_WORKER_NAME, "nb": number}}
    )
    click.echo(yaml.dump(result))
