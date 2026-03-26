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

	csvReader := csv.NewReader(file)
	betReader := NewBetReader(csvReader)

	errConn := c.createClientSocket()
	if errConn != nil {
		log.Errorf("action: connect | result: fail | client_id: %v | error: %v", c.config.ID, errConn)
		return
	}
	defer c.conn.Close()

	for {
		batch, errRead := betReader.ReadBatch(c.config.BatchAmount)

		if len(batch) == 0 && errRead == io.EOF {
			break
		}

		var dtos []BetDTO
		for _, bet := range batch {
			dtos = append(dtos, ConvertToDTO(bet))
		}

		agencyIDInt, errParse := strconv.Atoi(c.config.ID)
		if errParse != nil {
			log.Errorf("action: parse_agency_id | result: fail | client_id: %v | error: %v", c.config.ID, errParse)
			return
		}

		// Enviar batch entero
		err = SendBetBatch(c.conn, uint8(agencyIDInt), dtos)
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

	c.conn.Close()
	log.Infof("action: loop_finished | result: success | client_id: %v", c.config.ID)
}
