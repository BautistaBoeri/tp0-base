package common

import (
	"encoding/binary"
	"fmt"
	"net"
)

const SEND_BET_OP = 1
const ACK_OP = 2

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

// SendBetMessage empaqueta y envía la apuesta
func SendBetMessage(conn net.Conn, bet Bet) error {
	csv := fmt.Sprintf("%s,%s,%s,%s,%s", bet.FirstName, bet.LastName, bet.Document, bet.Birthdate, bet.Number)
	payload := []byte(csv)

	header := make([]byte, 5)
	header[0] = SEND_BET_OP
	binary.BigEndian.PutUint32(header[1:], uint32(len(payload)))

	packet := append(header, payload...)
	return sendAll(conn, packet)
}

// ReceiveAckMessage espera la respuesta del server
func ReceiveAckMessage(conn net.Conn) (string, error) {
	header, err := receiveAll(conn, 5)
	if err != nil {
		return "", err
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
