import socket
import struct

OPCODE_BET = 1
OPCODE_ACK = 2
OPCODE_BATCH = 3
OPCODE_ERROR = 9

class BetFormatError(ValueError):
    def __init__(self, message, batch_size):
        super().__init__(message)
        self.batch_size = batch_size

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

def recv_bet_batch(sock: socket.socket) -> list[BetDTO]:
    # Leemos el opcode y la cantidad de apuestas
    # El opcode es 1 byte (!B) y la cantidad N son 2 bytes enteros (!H de unsigned short)
    header_bytes = _recv_exact(sock, 3)
    
    opcode, batch_size = struct.unpack('!BH', header_bytes)
    
    if opcode != OPCODE_BATCH:
        raise ValueError(f"Opcode inesperado. Se esperaba {OPCODE_BATCH} pero llegó {opcode}")
        
    dtos = []
    
    for _ in range(batch_size):
        # 1. Leemos el tamaño de esta apuesta individual (2 bytes, !H)
        bet_len_bytes = _recv_exact(sock, 2)
        bet_len = struct.unpack('!H', bet_len_bytes)[0]
        
        # 2. Leemos la data en string
        payload_bytes = _recv_exact(sock, bet_len)
        mensaje = payload_bytes.decode('utf-8')
        campos = mensaje.split(',')
        
        if len(campos) != 5:
            raise BetFormatError(f"Formato de apuesta incorrecto. Se esperaban 5 campos, llegaron: {len(campos)}", batch_size)
            
        dto = BetDTO(
            first_name=campos[0],
            last_name=campos[1],
            document=campos[2],
            birthdate=campos[3],
            number=campos[4]
        )
        dtos.append(dto)

    return dtos

def send_ack(sock: socket.socket):
    payload = b"OK"
    header = struct.pack('!BI', OPCODE_ACK, len(payload))
    sock.sendall(header + payload)

def send_error(sock: socket.socket):
    payload = b"ERROR"
    header = struct.pack('!BI', OPCODE_ERROR, len(payload))
    sock.sendall(header + payload)
