# go-aws-mq
Go AWSMQ is a library to connect the AWSMQ in an effective way. This is strongly influenced from the article https://www.ribice.ba/golang-rabbitmq-client/

# How to connect?

```
logger := zerolog.New(os.Stderr).With().Timestamp().Logger()
sigs := make(chan os.Signal, 1)
client := awsmq.New("<pushqueueName","listnerQueueName","<amqpurl>",logger,sigs)
```

# How to Stream?

```
for {
		bctx := context.Background()
		ctx, _ := context.WithCancel(bctx)
		Stream(ctx, client)
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
			consumerName(i), // Consumer
			false,           // Auto-Ack
			false,           // Exclusive
			false,           // No-local
			false,           // No-Wait
			nil,             // Args
		)
		if err != nil {
			return err
		}

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
	l := c.Logger.Log().Timestamp()
	startTime := time.Now()
	fmt.Println(string(msg.Body))
	msg.Ack(true)
	//msg.Nack(false, true)
  defer func(ctx context.Context, e event, m amqp.Delivery, logger *zerolog.Event) {
		if err := recover(); err != nil {
			stack := make([]byte, 8096)
			stack = stack[:runtime.Stack(stack, false)]
			l.Bytes("stack", stack).Str("level", "fatal").Interface("error", err).Msg("panic recovery for rabbitMQ message")
			msg.Nack(false, false)
		}
	}(ctx, evt, msg, l)  
	return  
}  
  
  
```

# Loging and Negative acknowledgment
```
func logAndNack(msg amqp.Delivery, l *zerolog.Event, t time.Time, err string, args ...interface{}) {
	msg.Nack(false, false)
	l.Int64("took-ms", time.Since(t).Milliseconds()).Str("level", "error").Msg(fmt.Sprintf(err, args...))
}
```
# References: 
- https://gist.github.com/harrisonturton/c6b62d45e6117d5d03ff44e4e8e1e7f7
- https://gist.github.com/ribice/20951bd1c84d714ff2476465c0c0653f

