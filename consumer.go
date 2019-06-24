package ynsq

import (
	"errors"
	"fmt"
	"log"
	"os"

	"github.com/spaolacci/murmur3"
	nsq "github.com/youzan/go-nsq"
)

type defaultConsumeHandler struct {
	topic string
}

func (c *defaultConsumeHandler) HandleMessage(m *nsq.Message) error {
	//todo
	return nil
}

type Subscriber struct {
	Topic   string
	Channel string
	Handler nsq.Handler
}

//ConsumerClient .
type ConsumerClient struct {
	lookupAddresss []string
	config         *nsq.Config  `default:"-"`
	concurrency    int          `default:"5"`
	logLevel       nsq.LogLevel `default:"warning"` //debug info warning error
	consumers      map[string]*nsq.Consumer
	subscribers    []Subscriber
}

//NewConsumerClient .
func NewConsumerClient(lookupaddresss []string, subscribers []Subscriber) *ConsumerClient {
	if len(lookupaddresss) == 0 || len(subscribers) == 0 {
		panic("new producer client param error")
	}
	return &ConsumerClient{
		lookupAddresss: lookupaddresss,
		concurrency:    5,
		subscribers:    subscribers,
		consumers:      make(map[string]*nsq.Consumer),
		logLevel:       nsq.LogLevelWarning,
	}
}

//Init .
func (client *ConsumerClient) Init() {
	client.config = nsq.NewConfig()
	client.config.EnableOrdered = true
	client.config.Hasher = murmur3.New32()
	client.StartSubscribe()
}

//SetConcurrency .
func (client *ConsumerClient) SetConcurrency(concurrency int) {
	client.concurrency = concurrency
}

//SetLogLevel .
func (client *ConsumerClient) SetLogLevel(level string) {
	loglevel := nsq.LogLevelWarning
	switch level {
	case "debug":
		loglevel = nsq.LogLevelDebug
	case "info":
		loglevel = nsq.LogLevelInfo
	case "warning":
		loglevel = nsq.LogLevelWarning
	case "error":
		loglevel = nsq.LogLevelError
	default:
	}
	client.logLevel = loglevel
}

//StartSubscribe .
func (client *ConsumerClient) StartSubscribe() {
	for _, s := range client.subscribers {
		if s.Handler == nil {
			panic("message hanlder is nil")
		}
		consumer, err := nsq.NewConsumer(s.Topic, s.Channel, client.config)
		if err != nil {
			panic(err)
		}
		consumer.SetLogger(log.New(os.Stderr, "", log.LstdFlags), client.logLevel)
		consumer.AddConcurrentHandlers(s.Handler, client.concurrency)
		connectToLookupIsOk := false
		for _, lookupAddress := range client.lookupAddresss {
			if err = consumer.ConnectToNSQLookupd(lookupAddress); err == nil {
				connectToLookupIsOk = true
				break
			}
		}
		if !connectToLookupIsOk {
			panic(err)
		}
		topic := s.Topic
		client.consumers[topic] = consumer
	}
}

//AppendSubscribe .
func (client *ConsumerClient) AppendSubscribe(handler nsq.Handler, topic, channel string) error {
	if handler == nil {
		fmt.Errorf("message hanlder is nil.")
		return errors.New("message hanlder is nil.")
	}
	if _, ok := client.consumers[topic]; ok {
		fmt.Printf("%s topic is already subscribe.\n", topic)
		return nil
	}

	consumer, err := nsq.NewConsumer(topic, channel, client.config)
	if err != nil {
		return err
	}
	consumer.SetLogger(log.New(os.Stderr, "", log.LstdFlags), client.logLevel)
	// 注册并发消费处理函数

	consumer.AddConcurrentHandlers(handler, client.concurrency)
	connectToLookupIsOk := false
	for _, lookupAddress := range client.lookupAddresss {
		if err = consumer.ConnectToNSQLookupd(lookupAddress); err == nil {
			connectToLookupIsOk = true
			break
		}
	}
	if !connectToLookupIsOk {
		return err
	}

	return nil
}
