package nukafka

import (
	"context"
	"sync"
	"time"

	"encoding/json"

	"github.com/pkg/errors"
	"github.com/segmentio/kafka-go"
)

//PubConfig publisher config
type PubConfig struct {
	Brokers     []string
	Protocol    string
	Partition   int
	DialTimeout time.Duration
	SendTimeout time.Duration
}

//PubClient kafka client
type PubClient struct {
	pcfg  *PubConfig
	topic string
	wr    *kafka.Writer
	mux   sync.Mutex
}

//NewTopicPublisherCtx new topic publisher with context
//args:
//	ctx: passed context
//	topic: kafka specific topic
//	pcfg: ptr of kafka custom configuration
//returns:
//	client: kafka custom client
func NewTopicPublisherCtx(ctx context.Context, topic string, pcfg *PubConfig) *PubClient {
	wcfg := kafka.WriterConfig{
		Brokers: pcfg.Brokers,
		Topic:   topic,
	}
	p := &PubClient{wr: kafka.NewWriter(wcfg), pcfg: pcfg, topic: topic}
	return p
}

//SendCtx sends the data to Kafka
//args:
//	ctx: passed context
//	data: any
//returns:
//error:
//	error state
func (c *PubClient) SendCtx(ctx context.Context, data interface{}) error {
	c.mux.Lock()
	defer c.mux.Unlock()
	tctx, cancel := context.WithTimeout(ctx, c.pcfg.SendTimeout)
	defer cancel()
	echan := make(chan error, 1)
	go func(){
		bytes, err := json.Marshal(data)
		if err != nil {
			echan <- errors.Wrap(err, "fail on preparing data")
		}
		echan <- c.wr.WriteMessages(tctx, kafka.Message{
			Value: bytes,
		})
	}()
	select{
	case <- tctx.Done():
		cancel()
		return errors.New("send msg timeout")
	case err := <- echan:
		return err
	}
}

//Close close the kafka connection
//return:
//	error state
func (c *PubClient) Close() error {
	return c.wr.Close()
}
