import asyncio
import json
import socket
from asyncio.streams import StreamReader, StreamWriter
from typing import Any, Dict, Optional

HEADER_LEN = 64
ENCODING = "utf-8"
SOCKET_FAMILY = socket.AF_INET
SOCKET_TYPE = socket.SOCK_STREAM
DB_POLL_SLEEP_INTERVAL_MS = 1000
MAX_PROCS_PER_WORKER = 200
# Time to wait for handshake from worker
HANDSHAKE_TIMEOUT = 5.0


def get_socket_from_fd(fd: int) -> socket.socket:
    """Get the socket from the file descriptor."""
    return socket.fromfd(fd, SOCKET_FAMILY, SOCKET_TYPE)


async def receive_json(
    reader: StreamReader, timeout: Optional[float]
) -> Dict[str, Any]:
    """Read a JSON message from the stream.

    Two messages are expected, first a header with the length of the data,
    and then the data itself.

    :param timeout: The timeout in seconds to wait for each message

    :raises: TimeoutError if timeout is reached
    :raises: EOFError if the header is empty
    :raises: ValueError if the message is not a valid JSON
    """
    header = await asyncio.wait_for(reader.read(HEADER_LEN), timeout)
    if not header:
        raise EOFError("No header")
    data_len = int.from_bytes(header, "big")
    data = await asyncio.wait_for(reader.read(data_len), timeout)
    try:
        data = json.loads(data.decode(ENCODING))
    except Exception as exc:
        raise ValueError("Invalid JSON") from exc
    return data


async def send_json(writer: StreamWriter, dtype: str, data: Dict[str, Any]):
    """Send a JSON message to the stream.

    Two messages are sent, first a header with the length of the data,
    and then the data itself.
    """
    message = json.dumps({"type": dtype, "data": data}).encode(ENCODING)
    header = len(message).to_bytes(HEADER_LEN, "big")
    writer.write(header)
    writer.write(message)
    # TODO timeout?
    await writer.drain()
