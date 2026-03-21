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
	conn, err := net.Dial("tcp", c.config.ServerAddress)
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

	err = c.createClientSocket()
	if err != nil {
		return
	}
	defer c.conn.Close()

	agencyID, err := strconv.ParseUint(c.config.ID, 10, 8)
	if err != nil {
		log.Criticalf("action: parse_agency_id | result: fail | error: %v", err)
		return
	}

	csvReader := csv.NewReader(file)
	betReader := NewBetReader(csvReader)

	// Loop: enviar todos los batches por el mismo socket
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

		// Enviar batch entero
		err = SendBetBatch(c.conn, uint8(agencyID), batch)
		if err != nil {
			log.Errorf("action: send_message | result: fail | client_id: %v | error: %v", c.config.ID, err)
			return
		}

		_, err = ReceiveAckMessage(c.conn)
		if err != nil {
			log.Errorf("action: receive_message | result: fail | client_id: %v | error: %v", c.config.ID, err)
			return
		}

		log.Infof("action: batch_enviado | result: success | cantidad_apuestas: %d", len(batch))

		select {
		case <-ctx.Done():
			log.Infof("action: loop_finished | result: success | client_id: %v", c.config.ID)
			return
		case <-time.After(c.config.LoopPeriod):
		}
	}

	// Notificar al servidor que terminamos
	err = SendDone(c.conn, uint8(agencyID))
	if err != nil {
		log.Errorf("action: send_done | result: fail | client_id: %v | error: %v", c.config.ID, err)
		return
	}

	// Solicitar ganadores
	err = SendRequestWinners(c.conn, uint8(agencyID))
	if err != nil {
		log.Errorf("action: request_winners | result: fail | client_id: %v | error: %v", c.config.ID, err)
		return
	}

	// Esperar la lista de ganadores
	winners, err := ReceiveWinners(c.conn)
	if err != nil {
		log.Errorf("action: consulta_ganadores | result: fail | client_id: %v | error: %v", c.config.ID, err)
		return
	}

	log.Infof("action: consulta_ganadores | result: success | cant_ganadores: %d", len(winners))
}
