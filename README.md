# ynsq
client and server lib for  youzan nsq


## example

### producer
```go
package main

import (
	"fmt"
	"strconv"
	"time"

	"github.com/dw219422/ynsq"
)

var ptopics = []string{"to1"}
var lookupaddress = []string{"192.168.199.23:4161", "192.168.199.24:4161", "192.168.199.25:4161"}

func main() {
	client := ynsq.NewProducerClient(lookupaddress, ptopics)
	client.Init()
	go func() {
		for i := 0; i < 1000000000; i++ {
			index := (i + 1) % len(ptopics)
			topic := ptopics[index]
			body := []byte(ptopics[index] + "-" + strconv.Itoa(i))
			err := client.PublishMessage(topic, body)
			if err != nil {
				fmt.Printf("%d push %s to %s failed\n", i, topic, body)
			} else {
				fmt.Printf("%d push %s to %s success\n", i, topic, body)
			}
			time.Sleep(time.Millisecond * 500)
		}
	}()
	select {}
}

```

### consumer 
```go
package main

import (
	"fmt"
	"time"

	"github.com/dw219422/ynsq"

	nsq "github.com/youzan/go-nsq"
)

var lookupaddress = []string{"192.168.199.23:4161", "192.168.199.24:4161", "192.168.199.25:4161"}

type Topic struct {
	topic   string
	channel string
	handler nsq.Handler
}

type To1Handler struct {
	topic string
}

var total1 = 1

func (c *To1Handler) HandleMessage(m *nsq.Message) error {
	fmt.Printf("%d, topic:%s ------ body:%s, Timestamp:%d, Attempts:%d, NSQDAddress:%s, Partition:%s, Offset:%d, RawSize:%d\n",
		total1, c.topic, m.Body, m.Timestamp, m.Attempts, m.NSQDAddress, m.Partition, m.Offset, m.RawSize)
	total1++
	time.Sleep(time.Millisecond * 2000)
	return nil
}

type To2Handler struct {
	topic string
}

var total2 = 1

func (c *To2Handler) HandleMessage(m *nsq.Message) error {
	fmt.Printf("%d, topic:%s ------ body:%s, Timestamp:%d, Attempts:%d, NSQDAddress:%s, Partition:%s, Offset:%d, RawSize:%d\n",
		total1, c.topic, m.Body, m.Timestamp, m.Attempts, m.NSQDAddress, m.Partition, m.Offset, m.RawSize)
	total2++
	time.Sleep(time.Millisecond * 2000)
	return nil
}

func main() {
	subcribes := []ynsq.Subscriber{
		{"to1", "default", &To1Handler{"to1"}},
		{"to2", "default", &To2Handler{"to2"}},
	}
	client := ynsq.NewConsumerClient(lookupaddress, subcribes)
	client.SetConcurrency(1)
	client.Init()
	select {}

}

```
