import socket
import struct
from common.models import Bet

OPCODE_BET = 1
OPCODE_ACK = 2

def _recv_exact(sock: socket.socket, length: int) -> bytearray:
    data = bytearray()
    while len(data) < length:
        packet = sock.recv(length - len(data))
        if not packet:
            raise ConnectionError("Connection closed before reading all expected bytes")
        data.extend(packet)
    return data

def recv_bet(sock: socket.socket) -> Bet:
    header_bytes = _recv_exact(sock, 5)
    
    opcode, payload_length = struct.unpack('!BI', header_bytes)
    
    if opcode != OPCODE_BET:
        raise ValueError(f"Opcode inesperado. Se esperaba {OPCODE_BET}")
        
    payload_bytes = _recv_exact(sock, payload_length)
    mensaje = payload_bytes.decode('utf-8')
    campos = mensaje.split(',')
    
    if len(campos) != 5:
        raise ValueError(f"Formato de apuesta incorrecto. Se esperaban 5 campos, llegaron: {len(campos)}")
        
    return Bet(
        first_name=campos[0],
        last_name=campos[1],
        document=campos[2],
        birthdate=campos[3],
        number=campos[4]
    )

def send_ack(sock: socket.socket):
    payload = b"OK"
    header = struct.pack('!BI', OPCODE_ACK, len(payload))
    sock.sendall(header + payload)
