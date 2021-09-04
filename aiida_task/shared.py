import json
import socket
from typing import Any, Dict

HEADER_LEN = 64
ENCODING = "utf-8"
SOCKET_FAMILY = socket.AF_INET
SOCKET_TYPE = socket.SOCK_STREAM
HEARTBEAT_TIMEOUT_MS = 1000
HEARTBEAT_HEADER = b"!HEARTBEAT"
DISCONNECT_HEADER = b"!DISCONNECT"
DATABASE_POLL_MS = 100
MAX_PROCS_PER_WORKER = 200


def get_socket_from_fd(fd: int) -> socket.socket:
    """Get the socket from the file descriptor."""
    # by default this is None, and raises on exception on OSX,
    # socket.error: [Errno 35] Resource temporarily unavailable
    # there is probably a better way to do this
    socket.setdefaulttimeout(100000000)
    return socket.fromfd(fd, SOCKET_FAMILY, SOCKET_TYPE)


def send_data(conn: socket.socket, dtype: str, data: Dict[str, Any]):
    """Send JSONable data over a connection

    Two messages are sent, first a header with the length of the data,
    and then the data itself.
    """
    message = json.dumps({"type": dtype, "data": data}).encode(ENCODING)
    # send the header, with the message length
    conn.send(len(message).to_bytes(HEADER_LEN, "big"))
    # send the message
    conn.send(message)


def receive_data(conn: socket.socket) -> Dict[str, Any]:
    """Receive JSONable data over a connection

    First the header is received, with the length of the data,
    and then the data itself.
    """
    # read the header, with the message length
    header = conn.recv(HEADER_LEN)
    if not header:
        return {}
    message_length = int.from_bytes(header, "big")
    # read the message
    message = conn.recv(message_length)
    return json.loads(message.decode(ENCODING))
