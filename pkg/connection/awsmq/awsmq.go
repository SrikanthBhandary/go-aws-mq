package awsmq

import (
	"errors"
	"fmt"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/streadway/amqp"
)

var (
	ErrDIsConnected = errors.New("Disconnected from AWSMQ, trying to reconnect")
)

const (
	reconnectDelay = 5 * time.Second //Reconnecting the server after connection failure
	resendDelay    = 5 * time.Second //Resending the messages if the server didn't confirm
)

// Client holds necessery information for MQ.
// It can be used for both RMQ and AWSMQ.
type Client struct {
	PushQueue          string // Publisher Queue
	StreamQueue        string // Listner Queue
	Logger             zerolog.Logger
	connection         *amqp.Connection
	Channel            *amqp.Channel
	done               chan os.Signal
	notifyClose        chan *amqp.Error
	notifyConfirm      chan amqp.Confirmation
	IsConnected        bool
	alive              bool
	Threads            int
	Wg                 *sync.WaitGroup
	Exchange           string // Exchange
	DeadLetterExchange string // DeadLetterExchange
	DeadLetterQueue    string // DeadLetterQueueName
	RoutingKey         string // RoutingKey
	ExchangeType       string // ExchangeType
	sync.Mutex
}

// New is a constructor that takes address, push and listen queue names,
// logger, and a Channel that will notify rabbitmq client on server shutdown.
// We calculate the number of threads, create the client, and start the
// connection process. Connect method connects to the rabbitmq server and
// creates push/listen Channels if they doesn't exist.
func New(exchng, dlx, exchngType, dlq, routekey string, StreamQueue, PushQueue, addr string, l zerolog.Logger, done chan os.Signal) *Client {
	threads := runtime.GOMAXPROCS(0)
	if numCPU := runtime.NumCPU(); numCPU > threads {
		threads = numCPU
	}

	if exchng == "" {
		if PushQueue == "" {
			routekey = PushQueue
		} else {
			routekey = StreamQueue
		}
	}

	client := Client{
		Logger:             l,
		Threads:            threads,
		PushQueue:          PushQueue,
		StreamQueue:        StreamQueue,
		done:               done,
		alive:              true,
		Wg:                 &sync.WaitGroup{},
		Exchange:           exchng,
		DeadLetterExchange: dlx,
		DeadLetterQueue:    dlq,
		RoutingKey:         routekey,
		ExchangeType:       exchngType,
	}

	//client.Wg.Add(threads)

	go client.handleReconnect(addr)
	return &client
}

// handleReconnect will wait for a connection error on
// notifyClose, and then continuously attempt to reconnect.
func (c *Client) handleReconnect(addr string) {
	for c.alive {
		c.Lock()
		c.IsConnected = false
		c.Unlock()
		t := time.Now()
		fmt.Printf("Attempting to connect to AWSMQ: %s\n", addr)
		var retryCount int
		for !c.connect(addr) {
			if !c.alive {
				return
			}
			select {
			case <-c.done:
				return
			case <-time.After(reconnectDelay + time.Duration(retryCount)*time.Second):
				c.Logger.Printf("Disconnected from AWSMQ and failed to connect")
				retryCount++
			}
		}
		c.Logger.Printf("Connected to AWSMQ in: %vms", time.Since(t).Milliseconds())
		select {
		case <-c.done:
			return
		case <-c.notifyClose:
		}
	}
}

func (c *Client) BindQueues(ch *amqp.Channel) bool {
	//Binding the queue to exchange using the routing key.
	if err := ch.QueueBind(c.DeadLetterQueue, c.RoutingKey, c.DeadLetterExchange, false, nil); err != nil {
		c.Logger.Printf("cannot bind %v to %v: got: %v", c.DeadLetterQueue, c.DeadLetterExchange, err)
		return false
	}
	if err := ch.QueueBind(c.StreamQueue, c.RoutingKey, c.Exchange, false, nil); err != nil {
		c.Logger.Printf("cannot bind %v: got: %v", c.Exchange, err)
		return false
	}
	if err := ch.QueueBind(c.PushQueue, c.RoutingKey, c.Exchange, false, nil); err != nil {
		c.Logger.Printf("cannot bind %v: got: %v", c.Exchange, err)
		return false
	}
	return true
}

func (c *Client) DeclareQueues(ch *amqp.Channel) bool {
	if c.DeadLetterQueue != "" && c.DeadLetterExchange != "" {
		//Declaring all the deadletter queue
		options := amqp.Table{
			"x-dead-letter-exchange": c.DeadLetterExchange,
		}
		if _, err := ch.QueueDeclare(c.DeadLetterQueue, true, false, false, false, nil); err != nil {
			c.Logger.Printf("cannot declare %v: got: %v", c.DeadLetterQueue, err)
			return false
		}

		if c.StreamQueue != "" {
			if _, err := ch.QueueDeclare(c.StreamQueue, true, false, false, false, options); err != nil {
				c.Logger.Printf("cannot declare %v with dlq %v: got: %v", c.StreamQueue, c.DeadLetterExchange, err)
				return false
			}
		}

		if c.PushQueue != "" {
			if _, err := ch.QueueDeclare(c.PushQueue, true, false, false, false, options); err != nil {
				c.Logger.Printf("cannot declare %v with dlq %v: got: %v", c.PushQueue, c.DeadLetterExchange, err)
				return false
			}
		}

		return c.BindQueues(ch)
	} else {
		_, err := ch.QueueDeclare(c.StreamQueue, true, false, false, false, nil)
		if err != nil {
			c.Logger.Printf("Failed to declare stream queue: %v", err)
			return false
		}
		_, err = ch.QueueDeclare(c.PushQueue, true, false, false, false, nil)
		if err != nil {
			c.Logger.Printf("Failed to declare push queue: %v", err)
			return false
		}
	}
	return true
}

func (c *Client) DeclareExchanges(ch *amqp.Channel) bool {
	//DeclareExchanges method declares the exchanges if it is required.
	//It also create the deadletter exchange and this is optional.
	if c.Exchange != "" {
		if err := ch.ExchangeDeclare(c.Exchange, c.ExchangeType,
			false, true, false, false, nil); err != nil {
			c.Logger.Printf("cannot declare %v: got: %v", c.Exchange, err)
			return false
		}
	}

	//Declaring the deadletter exchange.
	if c.DeadLetterExchange != "" {
		if err := ch.ExchangeDeclare(c.DeadLetterExchange, c.ExchangeType,
			false, true, false, false, nil); err != nil {
			c.Logger.Printf("cannot declare %v: got: %v", c.DeadLetterExchange, err)
			return false
		}
	}
	return true
}

// connect will make a single attempt to connect to
// RabbitMq. It returns the success of the attempt.
func (c *Client) connect(addr string) bool {
	conn, err := amqp.Dial(addr)
	if err != nil {
		c.Logger.Printf("Failed to dial AWSMQ server: %v", err)
		return false
	}
	ch, err := conn.Channel()
	if err != nil {
		c.Logger.Printf("Failed connecting to Channel: %v", err)
		return false
	}
	ch.Confirm(false)
	ok := c.DeclareExchanges(ch)
	if !ok {
		c.Logger.Printf("Error occured, when declaring the exchange.")
		return false
	}

	ok = c.DeclareQueues(ch)
	if !ok {
		c.Logger.Printf("Error occured, when declaring the queues.")
		return false
	}

	c.changeConnection(conn, ch)
	c.Lock()
	c.IsConnected = true
	c.Unlock()
	return true
}

// changeConnection takes a new connection to the queue,
// and updates the Channel listeners to reflect this.
func (c *Client) changeConnection(connection *amqp.Connection, Channel *amqp.Channel) {
	c.connection = connection
	c.Channel = Channel
	c.notifyClose = make(chan *amqp.Error)
	c.notifyConfirm = make(chan amqp.Confirmation)
	c.Channel.NotifyClose(c.notifyClose)
	c.Channel.NotifyPublish(c.notifyConfirm)
}

// Push will push data onto the queue, and wait for a confirmation.
// If no confirms are received until within the resendTimeout,
// it continuously resends messages until a confirmation is received.
// This will block until the server sends a confirm.
func (c *Client) Push(data []byte) error {
	c.Lock()
	defer c.Unlock()
	if !c.IsConnected {

		return errors.New("Failed to push: not connected")
	}

	for {
		err := c.UnsafePush(data)
		if err != nil {
			if err == ErrDIsConnected {
				continue
			}
			return err
		}
		select {
		case confirm := <-c.notifyConfirm:
			if confirm.Ack {
				c.Logger.Printf("Message enqueued successfully.")
				return nil
			}
		case <-time.After(resendDelay):
		}
	}
}

// UnsafePush will push to the queue without checking for
// confirmation. It returns an error if it fails to connect.
// No guarantees are provided for whether the server will
// receive the message.
func (c *Client) UnsafePush(data []byte) error {
	if !c.IsConnected {
		return ErrDIsConnected
	}

	return c.Channel.Publish(
		c.Exchange,   // Exchange
		c.RoutingKey, // Routing key
		false,        // Mandatory
		false,        // Immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        data,
		},
	)
}

func (c *Client) Close() error {
	if !c.IsConnected {
		return nil
	}
	c.alive = false
	c.Logger.Printf("Waiting for current messages to be processed...")
	c.Wg.Wait()
	for i := 1; i <= c.Threads; i++ {
		fmt.Println("Closing consumer: ", i)
		err := c.Channel.Cancel(ConsumerName(i), false)
		if err != nil {
			return fmt.Errorf("Error canceling consumer %s: %v", ConsumerName(i), err)
		}
	}
	err := c.Channel.Close()
	if err != nil {
		return err
	}
	err = c.connection.Close()
	if err != nil {
		return err
	}
	c.IsConnected = false
	c.Logger.Printf("Gracefully stopped AWSMQ connection")
	return nil
}

func ConsumerName(i int) string {
	return fmt.Sprintf("go-consumer-%v", i)
}
