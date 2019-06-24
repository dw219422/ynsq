package ynsq

import (
	"log"
	"os"

	"github.com/spaolacci/murmur3"
	nsq "github.com/youzan/go-nsq"
)

type PushMessage struct {
	Topic   string
	Message []byte
}

//ProducerClient .
type ProducerClient struct {
	lookupAddresss []string
	config         *nsq.Config  `default:"-"`
	logLevel       nsq.LogLevel `default:"warning"` //debug info warning error
	// MessageChan    chan *PushMessage
	// exitChan       chan int
	// SuccessChan    chan bool
	producerMgr *nsq.TopicProducerMgr
	topics      []string
}

//NewProducerClient .
func NewProducerClient(lookupaddresss, topics []string) *ProducerClient {
	if len(lookupaddresss) == 0 || len(topics) == 0 {
		panic("new producer client param error")
	}
	return &ProducerClient{
		lookupAddresss: lookupaddresss,
		// MessageChan:    make(chan *PushMessage),
		// exitChan:       make(chan int),
		// SuccessChan:    make(chan bool),
		// topicConnectMgr: make(map[string]*nsq.TopicProducerMgr),
		logLevel: nsq.LogLevelWarning,
		topics:   topics,
	}
}

//NewTopicProducerMgr .
func (client *ProducerClient) NewTopicProducerMgr() {
	pubMgr, err := nsq.NewTopicProducerMgr(client.topics, client.config)
	if err != nil {
		panic(err)
	}
	client.producerMgr = pubMgr
}

//ConnectToNSQLookupd .
func (client *ProducerClient) ConnectToNSQLookupd() {
	connectToLookupIsOk := false
	for _, lookupAddress := range client.lookupAddresss {
		if client.producerMgr.ConnectToNSQLookupd(lookupAddress) == nil {
			connectToLookupIsOk = true
			break
		}
	}
	if !connectToLookupIsOk {
		panic("connect to nsqlookup failed")
	}
}

//Init .
func (client *ProducerClient) Init() {
	client.config = nsq.NewConfig()
	client.config.EnableOrdered = true
	client.config.Hasher = murmur3.New32()
	client.NewTopicProducerMgr()
	client.ConnectToNSQLookupd()
	//	client.SetLogLevel()
}

//SetLogLevel .
func (client *ProducerClient) SetLogLevel(level string) {
	switch level {
	case "debug":
		client.logLevel = nsq.LogLevelDebug
	case "info":
		client.logLevel = nsq.LogLevelInfo
	case "warning":
		client.logLevel = nsq.LogLevelWarning
	case "error":
		client.logLevel = nsq.LogLevelError
	default:
		client.logLevel = nsq.LogLevelWarning
	}
	client.producerMgr.SetLogger(log.New(os.Stderr, "", log.LstdFlags), client.logLevel)
}

//PublishOrdered .
func (client *ProducerClient) PublishOrdered(topic string, partitionKey, body []byte) error {
	_, _, _, err := client.producerMgr.PublishOrdered(topic, partitionKey, body)
	return err
}

//PublishMessage .
func (client *ProducerClient) PublishMessage(topic string, body []byte) error {
	return client.PublishOrdered(topic, body, body)
}
