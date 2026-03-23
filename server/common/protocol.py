import socket
import struct

OPCODE_BET = 1
OPCODE_ACK = 2

class BetFormatError(ValueError):
    pass

class BetDTO:
    def __init__(self, first_name: str, last_name: str, document: str, birthdate: str, number: str):
        self.first_name = first_name
        self.last_name = last_name
        self.document = document
        self.birthdate = birthdate
        self.number = number

def _recv_exact(sock: socket.socket, length: int) -> bytearray:
    data = bytearray()
    while len(data) < length:
        packet = sock.recv(length - len(data))
        if not packet:
            raise ConnectionError("Connection closed before reading all expected bytes")
        data.extend(packet)
    return data

def recv_bet(sock: socket.socket) -> BetDTO:
    header_bytes = _recv_exact(sock, 5)
    
    opcode, payload_length = struct.unpack('!BI', header_bytes)
    
    if opcode != OPCODE_BET:
        raise ValueError(f"Opcode inesperado. Se esperaba {OPCODE_BET}")
        
    payload_bytes = _recv_exact(sock, payload_length)
    mensaje = payload_bytes.decode('utf-8')
    campos = mensaje.split(',')
    
    if len(campos) != 5:
        raise BetFormatError(f"Formato de apuesta incorrecto. Se esperaban 5 campos, llegaron: {len(campos)}")
        
    return BetDTO(
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
