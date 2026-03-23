package common

import (
	"context"
	"encoding/csv"
	"io"
	"net"
	"os"
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

	csvReader := csv.NewReader(file)
	betReader := NewBetReader(csvReader)

	for {
		batch, errRead := betReader.ReadBatch(c.config.BatchAmount)

		// Si no hay más apuestas para leer, salimos  (Finalizó todo el archivo)
		if len(batch) == 0 && errRead == io.EOF {
			break
		}

		err := c.createClientSocket()
		if err != nil {
			select {
			case <-ctx.Done():
				return
			case <-time.After(c.config.LoopPeriod):
				continue
			}
		}

		var dtos []BetDTO
		for _, bet := range batch {
			dtos = append(dtos, ConvertToDTO(bet))
		}

		// Enviar batch entero
		err = SendBetBatch(c.conn, dtos)
		if err != nil {
			log.Errorf("action: send_message | result: fail | client_id: %v | error: %v", c.config.ID, err)
			c.conn.Close()
			return
		}

		_, err = ReceiveAckMessage(c.conn)
		c.conn.Close()

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
	log.Infof("action: loop_finished | result: success | client_id: %v", c.config.ID)
}
