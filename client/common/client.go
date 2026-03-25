package common

import (
	"context"
	"net"
	"time"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

type ClientConfig struct {
	ID            string
	ServerAddress string
	LoopAmount    int
	LoopPeriod    time.Duration
	Bet           Bet
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
	for msgID := 1; msgID <= c.config.LoopAmount; msgID++ {
		err := c.createClientSocket()
		if err != nil {
			select {
			case <-ctx.Done():
				return
			case <-time.After(c.config.LoopPeriod):
				continue
			}
		}

		dto := BetDTO{
			FirstName: c.config.Bet.FirstName,
			LastName:  c.config.Bet.LastName,
			Document:  c.config.Bet.Document,
			Birthdate: c.config.Bet.Birthdate,
			Number:    c.config.Bet.Number,
		}

		err = SendBetMessage(c.conn, dto)
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

		log.Infof("action: apuesta_enviada | result: success | dni: %s | numero: %s", c.config.Bet.Document, c.config.Bet.Number)

		select {
		case <-ctx.Done():
			log.Infof("action: loop_finished | result: success | client_id: %v", c.config.ID)
			return
		case <-time.After(c.config.LoopPeriod):
		}
	}
	log.Infof("action: loop_finished | result: success | client_id: %v", c.config.ID)
}
