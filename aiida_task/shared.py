import socket

HEADER = 64
FORMAT = "utf-8"
DISCONNECT_MESSAGE = "!DISCONNECT"
SOCKET_FAMILY = socket.AF_INET
SOCKET_TYPE = socket.SOCK_STREAM
HEARTBEAT_TIMEOUT_MS = 1000


def get_socket_from_fd(fd: int) -> socket.socket:
    """Get the socket from the file descriptor."""
    # by default this is None, and raises on exception on OSX,
    # socket.error: [Errno 35] Resource temporarily unavailable
    # there is probably a better way to do this
    socket.setdefaulttimeout(100000000)
    return socket.fromfd(fd, SOCKET_FAMILY, SOCKET_TYPE)
