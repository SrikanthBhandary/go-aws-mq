package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/SrikanthBhandary/go-aws-mq/pkg/connection/awsmq"
	"github.com/rs/zerolog"
	"github.com/streadway/amqp"
)

func main() {
	logger := zerolog.New(os.Stderr).With().Timestamp().Logger()
	//(StreamQueue, PushQueue, addr string, l zerolog.Logger, done chan os.Signal
	sigs := make(chan os.Signal, 1)

	client := awsmq.New(
		"AWSMQ-Test-Exchange",                //ExchangeName
		"AWSMQ-Test-Exchange-DeadLetter",     //DeadLetterExchangeName
		"direct",                             //TypeofExchange
		"AWSMQ-Test-Queue-DeadLetter",        //DeadLetterQueueName
		"AWSMQ",                              //Routingkey
		"",                                   //PushQueue
		"AWSMQ-Test-Queue-Input",             //StreamQueue
		"amqp://user:bitnami@localhost:5672", //AMQP URL
		logger,                               //Logger
		sigs,                                 //Signal
	)

	for {
		bctx := context.Background()
		ctx, _ := context.WithCancel(bctx)
		Stream(ctx, client)
	}

}

// Stream should be handled in a different way if we are making this as a lib.
func Stream(cancelCtx context.Context, c *awsmq.Client) error {
	for {
		if c.IsConnected {
			break
		}
		time.Sleep(1 * time.Second)
	}

	err := c.Channel.Qos(1, 0, false)
	if err != nil {
		return err
	}

	var connectionDropped bool
	for i := 1; i <= c.Threads; i++ {
		msgs, err := c.Channel.Consume(
			c.StreamQueue,
			awsmq.ConsumerName(i), // Consumer
			false,                 // Auto-Ack
			false,                 // Exclusive
			false,                 // No-local
			false,                 // No-Wait
			nil,                   // Args
		)
		if err != nil {
			return err
		}
		c.Wg.Add(1)
		go func() {
			defer c.Wg.Done()
			for {
				select {
				case <-cancelCtx.Done():
					return
				case msg, ok := <-msgs:
					if !ok {
						connectionDropped = true
						return
					}
					parseEvent(msg, c, cancelCtx)
				}
			}
		}()

	}

	c.Wg.Wait()

	if connectionDropped {
		return errors.New("DIsConnected from AWSMQ, trying to reconnect")
	}

	return nil
}

func parseEvent(msg amqp.Delivery, c *awsmq.Client, ctx context.Context) {
	fmt.Println(string(msg.Body))
	//msg.Ack(true) To test positive acknowldegement
	msg.Nack(false, false) // To move data to deadletter queue.

	return
}
