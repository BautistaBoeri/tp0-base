import socket
import struct
from common.models import Bet

OPCODE_BET = 1
OPCODE_ACK = 2
OPCODE_BATCH = 3
OPCODE_DONE = 4
OPCODE_WINNERS = 5
OPCODE_ERROR = 9

class BetFormatError(ValueError):
    def __init__(self, message, batch_size):
        super().__init__(message)
        self.batch_size = batch_size

def _recv_exact(sock: socket.socket, length: int) -> bytearray:
    data = bytearray()
    while len(data) < length:
        packet = sock.recv(length - len(data))
        if not packet:
            raise ConnectionError("Connection closed before reading all expected bytes")
        data.extend(packet)
    return data

def recv_message(sock: socket.socket):
    """
    Reads the next message from the socket.
    Returns:
      ('batch', agency_id, list[Bet])  if it's a BATCH message
      ('done', agency_id, None)        if it's a DONE message
    Raises ValueError on unknown opcode.
    """
    # Header: opcode (1 byte) + agency_id (1 byte) + count/unused (2 bytes)
    header_bytes = _recv_exact(sock, 4)
    opcode, agency_id, count = struct.unpack('!BBH', header_bytes)

    if opcode == OPCODE_DONE:
        return ('done', agency_id, None)

    if opcode == OPCODE_BATCH:
        batch = []
        for _ in range(count):
            bet_len_bytes = _recv_exact(sock, 2)
            bet_len = struct.unpack('!H', bet_len_bytes)[0]

            payload_bytes = _recv_exact(sock, bet_len)
            mensaje = payload_bytes.decode('utf-8')
            campos = mensaje.split(',')

            if len(campos) != 5:
                raise BetFormatError(
                    f"Formato de apuesta incorrecto. Se esperaban 5 campos, llegaron: {len(campos)}",
                    count
                )

            bet = Bet(
                first_name=campos[0],
                last_name=campos[1],
                document=campos[2],
                birthdate=campos[3],
                number=campos[4]
            )
            batch.append(bet)
        return ('batch', agency_id, batch)

    raise ValueError(f"Opcode desconocido: {opcode}")

def send_ack(sock: socket.socket):
    payload = b"OK"
    header = struct.pack('!BI', OPCODE_ACK, len(payload))
    sock.sendall(header + payload)

def send_error(sock: socket.socket):
    payload = b"ERROR"
    header = struct.pack('!BI', OPCODE_ERROR, len(payload))
    sock.sendall(header + payload)

def send_winners(sock: socket.socket, winners: list[str]):
    """
    Sends the list of winner DNIs to the client.
    Format: opcode (1B) + count (2B) + for each DNI: len(2B) + dni bytes
    """
    header = struct.pack('!BH', OPCODE_WINNERS, len(winners))
    payload = bytearray(header)
    for dni in winners:
        encoded = dni.encode('utf-8')
        payload += struct.pack('!H', len(encoded))
        payload += encoded
    sock.sendall(bytes(payload))
