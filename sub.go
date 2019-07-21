package nukafka

import (
	"log"
	"bytes"
	"context"
	"fmt"
	"sync"
	"time"

	"encoding/json"

	"github.com/segmentio/kafka-go"
)

//ConsumerFunc run function for consumer specific topic
type ConsumerFunc func(ctx context.Context, message Message)

//SubConfig subscriber config
type SubConfig struct {
	Brokers     []string
	Protocol    string
	Partition   int
	MinBytes    int
	MaxBytes    int
	GroupID     string
	MaxInFlight int   //default 100
	Offset      int64 //optional 0 = default
	DialTimeout time.Duration
	SendTimeout time.Duration
}

//SubClient a subcsriber client
type SubClient struct {
	topic  string
	pclt   *PubClient
	scfg   *SubConfig
	cf     ConsumerFunc
	fcount *fcount
	rdr    *kafka.Reader
	rerdr  *kafka.Reader
	retry  bool
	done   bool
}

type fcount struct {
	flightc int
	mux     sync.Mutex
}

//NewTopicConsumerCtx creates new consumer for a specific topic
func NewTopicConsumerCtx(ctx context.Context, topic string, retrySupport bool, scfg *SubConfig) *SubClient {
	pcfg := &PubConfig{
		Brokers:     scfg.Brokers,
		Protocol:    scfg.Protocol,
		Partition:   scfg.Partition,
		SendTimeout: scfg.SendTimeout,
		DialTimeout: scfg.DialTimeout,
	}
	rdr := newReader(topic, scfg)
	sclt := &SubClient{topic: topic, scfg: scfg, fcount: new(fcount), rdr: rdr}
	if retrySupport {
		rtopic := fmt.Sprintf("retry_%s_%s", topic, scfg.GroupID)
		p := NewTopicPublisherCtx(ctx, rtopic, pcfg)
		rerdr := newReader(rtopic, scfg)
		sclt.pclt = p
		sclt.rerdr = rerdr
	}
	return sclt
}

func newReader(topic string, scfg *SubConfig) *kafka.Reader {
	sconfig := kafka.ReaderConfig{
		Brokers:         scfg.Brokers,
		Topic:           topic,
		GroupID:         scfg.GroupID,
		MinBytes:        scfg.MinBytes,
		MaxBytes:        scfg.MaxBytes,
		MaxWait:         1 * time.Second,
		ReadLagInterval: -1,
	}
	rdr := kafka.NewReader(sconfig)
	if offs := scfg.Offset; offs > 0 {
		rdr.SetOffset(offs)
	}
	return rdr
}

//RegisterFunc register listener function for a topic
func (s *SubClient) RegisterFunc(cf ConsumerFunc) {
	s.cf = cf
}

func (s *SubClient) runCtx(ctx context.Context) error {
	for {
		if s.done {
			return nil
		}
		if s.fcount.flightc <= s.scfg.MaxInFlight {
			m, err := s.rdr.FetchMessage(context.Background())
			if err != nil {
				log.Println("fetch message error", err.Error())
				continue
			}
			value := m.Value
			msg := Message{pclient: s.pclt, Value: value, Topic: m.Topic}
			s.fcount.flightc++
			go s.runFuncCtx(ctx, false, msg, &m)
		}
	}
}

func (s *SubClient) runRetCtx(ctx context.Context) error {
	if s.rerdr == nil {
		return nil
	}
	for {
		if s.done {
			return nil
		}
		m, err := s.rerdr.FetchMessage(context.Background())
		if err != nil {
			log.Println("fetch retry message error", err.Error())
			continue
		}
		value := m.Value
		//pending message
		usePending := true
		var pm PendingMessage
		dec := json.NewDecoder(bytes.NewReader(value))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&pm); err != nil {
			usePending = false
		}
		if usePending {
			u, err := time.Parse(time.RFC3339, pm.ProcessAfter)
			if err != nil {
				log.Println("error parsing pending message:", err.Error())
				if err := s.rdr.CommitMessages(ctx, m); err != nil{
					log.Println("unable to commit retry message:", err.Error())
				}
				continue
			}
			if !time.Now().After(u) {
				go func(p *PubClient, pmsg PendingMessage) {
					var sent bool
					for !sent {
						if err := p.SendCtx(ctx, pmsg); err != nil {
							log.Println("error sending pending message", err.Error())
						}
						sent = true
					}
				}(s.pclt, pm)
				if err := s.rdr.CommitMessages(ctx, m); err != nil{
					log.Println("unable to commit retry message:", err.Error())
				}
				continue
			}
			msg := Message{pclient: s.pclt, Value: value, Topic: m.Topic}
			value, _ = json.Marshal(pm.OriginalMessage)
			msg.Value = value
			msg.IsRetryMsg = true
			msg.RetryAttempt = pm.ProcessRetryAttempt + 1
			go s.runFuncCtx(ctx, true, msg, &m)
		}
	}
	return nil
}

//RunCtx runs the consumer listener
func (s *SubClient) RunCtx(ctx context.Context) error {
	rerr := make(chan error, 1)
	reterr := make(chan error, 1)
	go func() {
		rerr <- s.runCtx(ctx)
	}()
	go func() {
		reterr <- s.runRetCtx(ctx)
	}()
	<-rerr
	<-reterr
	log.Println("consumer(s) stopped")
	return nil
}

//Stop stops the runner and close the reader
func (s *SubClient) Stop() {
	s.done = true
	s.rdr.Close()
	s.rerdr.Close()
}

func (s *SubClient) runFuncCtx(ctx context.Context, retry bool, m Message, kmsg *kafka.Message) {
	if !retry {
		defer func() {
			s.fcount.flightc--
		}()
	}
	s.cf(ctx, m)
	if err := s.rerdr.CommitMessages(ctx, *kmsg); err != nil{
		log.Println("unable to commit message", err.Error())
	}
}
