package common

import (
	"context"
	"encoding/csv"
	"io"
	"net"
	"os"
	"strconv"
	"time"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

type ClientConfig struct {
	ID            string
	ServerAddress string
	LoopAmount    int
	LoopPeriod    time.Duration
	BatchAmount   int
}

type Client struct {
	config ClientConfig
	conn   net.Conn
}

func NewClient(config ClientConfig) *Client {
	client := &Client{
		config: config,
	}
	return client
}

func (c *Client) createClientSocket() error {
	var conn net.Conn
	var err error
	// Intento de reconexiones en caso del que el servidor  no se haya levantado aun
	for i := 0; i < 3; i++ {
		conn, err = net.Dial("tcp", c.config.ServerAddress)
		if err != nil {
			time.Sleep(300 * time.Millisecond)
			continue
		}
		break
	}
	if err != nil {
		log.Criticalf(
			"action: connect | result: fail | client_id: %v | error: %v",
			c.config.ID,
			err,
		)
		return err
	}
	c.conn = conn
	return nil
}

func (c *Client) StartClientLoop(ctx context.Context) {
	file, err := os.Open("/dataset.csv")
	if err != nil {
		log.Criticalf("action: open_file | result: fail | error: %v", err)
		return
	}
	defer file.Close()

	agencyID, err := strconv.ParseUint(c.config.ID, 10, 8)
	if err != nil {
		log.Criticalf("action: parse_agency_id | result: fail | error: %v", err)
		return
	}

	csvReader := csv.NewReader(file)
	betReader := NewBetReader(csvReader)

	// Loop: crear una conexión nueva por cada batch, mandarlo y cerrarla
	for {
		select {
		case <-ctx.Done():
			log.Infof("action: loop_finished | result: success | client_id: %v", c.config.ID)
			return
		default:
		}

		batch, errRead := betReader.ReadBatch(c.config.BatchAmount)

		// Si no hay más apuestas para leer, terminamos el loop
		if len(batch) == 0 && errRead == io.EOF {
			break
		}

		err = c.createClientSocket()
		if err != nil {
			return
		}

		// Convertir el modelo de negocio Bet a DTO para la capa de red
		var dtos []BetDTO
		for _, bet := range batch {
			dtos = append(dtos, ConvertToDTO(bet))
		}

		// Enviar batch entero
		err = SendBetBatch(c.conn, uint8(agencyID), dtos)
		if err != nil {
			log.Errorf("action: send_message | result: fail | client_id: %v | error: %v", c.config.ID, err)
			c.conn.Close()
			return
		}

		_, err = ReceiveAckMessage(c.conn)
		if err != nil {
			log.Errorf("action: receive_message | result: fail | client_id: %v | error: %v", c.config.ID, err)
			c.conn.Close()
			return
		}

		c.conn.Close()

		log.Infof("action: batch_enviado | result: success | cantidad_apuestas: %d", len(batch))

		select {
		case <-ctx.Done():
			log.Infof("action: loop_finished | result: success | client_id: %v", c.config.ID)
			return
		case <-time.After(c.config.LoopPeriod):
		}
	}

	// Notificar al servidor que terminamos
	err = c.createClientSocket()
	if err != nil {
		return
	}
	err = SendDone(c.conn, uint8(agencyID))
	if err != nil {
		log.Errorf("action: send_done | result: fail | client_id: %v | error: %v", c.config.ID, err)
		c.conn.Close()
		return
	}
	_, err = ReceiveAckMessage(c.conn)
	if err != nil {
		log.Errorf("action: receive_done_ack | result: fail | client_id: %v | error: %v", c.config.ID, err)
		c.conn.Close()
		return
	}
	c.conn.Close()

	// Pedir ganadores
	err = c.createClientSocket()
	if err != nil {
		return
	}
	err = SendRequestWinners(c.conn, uint8(agencyID))
	if err != nil {
		log.Errorf("action: send_request_winners | result: fail | client_id: %v | error: %v", c.config.ID, err)
		c.conn.Close()
		return
	}

	// Esperar la lista de ganadores
	winners, err := ReceiveWinners(c.conn)
	if err != nil {
		log.Errorf("action: consulta_ganadores | result: fail | client_id: %v | error: %v", c.config.ID, err)
		c.conn.Close()
		return
	}
	c.conn.Close()

	log.Infof("action: consulta_ganadores | result: success | cant_ganadores: %d", len(winners))
}
