package common

import (
	"encoding/binary"
	"fmt"
	"net"
)

const SEND_BET_OP = 1
const ACK_OP = 2
const SEND_BATCH_OP = 3
const DONE_OP = 4
const WINNERS_OP = 5
const ERROR_OP = 9

// sendAll asegura que se envíen todos los bytes sin short-writes
func sendAll(conn net.Conn, data []byte) error {
	totalSent := 0
	for totalSent < len(data) {
		sent, err := conn.Write(data[totalSent:])
		if err != nil {
			return fmt.Errorf("error in sendAll: %v", err)
		}
		totalSent += sent
	}
	return nil
}

// receiveAll asegura que se lean exactamente length bytes sin short-reads
func receiveAll(conn net.Conn, length int) ([]byte, error) {
	data := make([]byte, length)
	totalRead := 0

	for totalRead < length {
		read, err := conn.Read(data[totalRead:])
		if err != nil {
			return nil, fmt.Errorf("error in receiveAll: %v", err)
		}
		totalRead += read
	}
	return data, nil
}

// SendBetMessage empaqueta de forma individual UNA sola apuesta.
// Devuelve el slice compuesto por [2 bytes: Largo de la apuesta] + [Datos de la apuesta en CSV]
func SerializeBet(bet Bet) []byte {
	csv := fmt.Sprintf("%s,%s,%s,%s,%s", bet.FirstName, bet.LastName, bet.Document, bet.Birthdate, bet.Number)
	payload := []byte(csv)

	header := make([]byte, 2)
	binary.BigEndian.PutUint16(header, uint16(len(payload)))

	return append(header, payload...)
}

// SendBetBatch envía un lote (batch) de apuestas en un solo paquete.
// Protocolo: [1 byte: SEND_BATCH_OP] + [1 byte: agency_id] + [2 bytes: Cantidad N] + [N Apuestas serializadas]
func SendBetBatch(conn net.Conn, agencyID uint8, batch []Bet) error {
	header := make([]byte, 4)
	header[0] = SEND_BATCH_OP
	header[1] = agencyID
	binary.BigEndian.PutUint16(header[2:], uint16(len(batch)))

	var payload []byte
	for _, bet := range batch {
		serializedBet := SerializeBet(bet)
		payload = append(payload, serializedBet...)
	}

	packet := append(header, payload...)
	return sendAll(conn, packet)
}

// SendDone notifica al servidor que esta agencia terminó de enviar apuestas.
// Protocolo: [1 byte: DONE_OP] + [1 byte: agency_id] + [2 bytes: 0 (unused)]
func SendDone(conn net.Conn, agencyID uint8) error {
	header := make([]byte, 4)
	header[0] = DONE_OP
	header[1] = agencyID
	binary.BigEndian.PutUint16(header[2:], 0)
	return sendAll(conn, header)
}

// ReceiveWinners lee la lista de DNI ganadores enviada por el servidor.
// Protocolo: [1 byte: WINNERS_OP] + [2 bytes: cantidad] + [por cada DNI: 2 bytes largo + bytes DNI]
func ReceiveWinners(conn net.Conn) ([]string, error) {
	header, err := receiveAll(conn, 3)
	if err != nil {
		return nil, err
	}
	if header[0] != WINNERS_OP {
		return nil, fmt.Errorf("expected WINNERS_OP, got %d", header[0])
	}
	count := int(binary.BigEndian.Uint16(header[1:]))

	winners := make([]string, 0, count)
	for i := 0; i < count; i++ {
		lenBytes, err := receiveAll(conn, 2)
		if err != nil {
			return nil, err
		}
		dniLen := int(binary.BigEndian.Uint16(lenBytes))
		dniBytes, err := receiveAll(conn, dniLen)
		if err != nil {
			return nil, err
		}
		winners = append(winners, string(dniBytes))
	}
	return winners, nil
}

// ReceiveAckMessage espera la respuesta del server
func ReceiveAckMessage(conn net.Conn) (string, error) {
	header, err := receiveAll(conn, 5)
	if err != nil {
		return "", err
	}

	if header[0] == ERROR_OP {
		return "", fmt.Errorf("server_returned_error")
	}

	if header[0] != ACK_OP {
		return "", fmt.Errorf("invalid opcode received")
	}

	payloadLength := binary.BigEndian.Uint32(header[1:])
	payload, err := receiveAll(conn, int(payloadLength))
	if err != nil {
		return "", err
	}

	return string(payload), nil
}
