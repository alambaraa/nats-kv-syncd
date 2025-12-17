package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
)

type Operation struct {
	Op     string  `json:"op"`
	Bucket string  `json:"bucket"`
	Key    string  `json:"key"`
	Value  *string `json:"value,omitempty"`
	TS     int64   `json:"ts"`
	NodeID string  `json:"node_id"`
}

type Meta struct {
	TS        int64  `json:"ts"`
	NodeID    string `json:"node_id"`
	Tombstone bool   `json:"tombstone"`
}

type Suppressor struct {
	mu sync.Mutex
	m  map[string]int
}

func NewSuppressor() *Suppressor { return &Suppressor{m: map[string]int{}} }

func (s *Suppressor) Add(key string) {
	s.mu.Lock()
	s.m[key]++
	s.mu.Unlock()
}

func (s *Suppressor) Done(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.m[key] <= 1 {
		delete(s.m, key)
		return
	}
	s.m[key]--
}

func (s *Suppressor) Has(key string) bool {
	s.mu.Lock()
	_, ok := s.m[key]
	s.mu.Unlock()
	return ok
}

type Clock struct {
	mu    sync.Mutex
	last  int64
	meta  nats.KeyValue
	ckKey string
}

func LoadClock(metaKV nats.KeyValue, nodeID string) *Clock {
	ckKey := "__clock." + nodeID
	var last int64
	e, err := metaKV.Get(ckKey)
	if err == nil {
		if v, e2 := strconv.ParseInt(string(e.Value()), 10, 64); e2 == nil {
			last = v
		}
	}
	return &Clock{last: last, meta: metaKV, ckKey: ckKey}
}

func (c *Clock) Next() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now().UnixNano()
	if now <= c.last {
		c.last++
	} else {
		c.last = now
	}
	_, _ = c.meta.Put(c.ckKey, []byte(strconv.FormatInt(c.last, 10)))
	return c.last
}

func ensureKV(js nats.JetStreamContext, bucket string) (nats.KeyValue, error) {
	kv, err := js.KeyValue(bucket)
	if err == nil {
		return kv, nil
	}
	if !errors.Is(err, nats.ErrBucketNotFound) {
		return nil, err
	}
	return js.CreateKeyValue(&nats.KeyValueConfig{Bucket: bucket})
}

func ensureStream(js nats.JetStreamContext, streamName, subj string) error {
	if _, err := js.StreamInfo(streamName); err == nil {
		return nil
	}
	_, err := js.AddStream(&nats.StreamConfig{
		Name:     streamName,
		Subjects: []string{subj},
		Storage:  nats.FileStorage,
		Retention: nats.LimitsPolicy,
		MaxAge:    7 * 24 * time.Hour,
	})
	if err == nil {
		return nil
	}
	if strings.Contains(strings.ToLower(err.Error()), "already") {
		return nil
	}
	return err
}

func readMeta(metaKV nats.KeyValue, key string) Meta {
	e, err := metaKV.Get(key)
	if err != nil {
		return Meta{}
	}
	var m Meta
	if json.Unmarshal(e.Value(), &m) != nil {
		return Meta{}
	}
	return m
}

func writeMeta(metaKV nats.KeyValue, key string, m Meta) {
	b, _ := json.Marshal(m)
	_, _ = metaKV.Put(key, b)
}

func remoteWins(remoteTS int64, remoteNode string, local Meta) bool {
	if remoteTS > local.TS {
		return true
	}
	if remoteTS < local.TS {
		return false
	}
	return remoteNode > local.NodeID
}

func main() {
	var (
		natsURL   = flag.String("nats-url", "nats://localhost:4222", "NATS local (KV)")
		repURL    = flag.String("rep-url", "", "NATS hub para rep.kv.ops (si vacío, usa nats-url)")
		bucket    = flag.String("bucket", "config", "KV bucket")
		nodeID    = flag.String("node-id", "", "Node ID")
		repSubj   = flag.String("rep-subj", "rep.kv.ops", "Replication subject")
		repStream = flag.String("rep-stream", "REP_KV_OPS", "JetStream stream para repSubj")
		reconEvery = flag.Duration("reconcile-every", 5*time.Minute, "Anti-entropy (0 desactiva)")
	)
	flag.Parse()

	if *nodeID == "" {
		log.Fatal("Falta --node-id")
	}
	if *repURL == "" {
		*repURL = *natsURL
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	ncLocal, err := nats.Connect(*natsURL, nats.Name("nats-kv-syncd:"+*nodeID))
	if err != nil {
		log.Fatalf("nats local: %v", err)
	}
	defer ncLocal.Drain()

	jsLocal, err := ncLocal.JetStream()
	if err != nil {
		log.Fatalf("js local: %v", err)
	}

	ncRep := ncLocal
	if *repURL != *natsURL {
		ncRep, err = nats.Connect(*repURL, nats.Name("nats-kv-syncd-rep:"+*nodeID))
		if err != nil {
			log.Fatalf("nats rep: %v", err)
		}
		defer ncRep.Drain()
	}
	jsRep, err := ncRep.JetStream()
	if err != nil {
		log.Fatalf("js rep: %v", err)
	}

	dataKV, err := ensureKV(jsLocal, *bucket)
	if err != nil {
		log.Fatalf("kv %s: %v", *bucket, err)
	}

	metaKV, err := ensureKV(jsLocal, *bucket+"_meta")
	if err != nil {
		log.Fatalf("meta kv: %v", err)
	}

	clock := LoadClock(metaKV, *nodeID)
	suppress := NewSuppressor()

	if err := ensureStream(jsRep, *repStream, *repSubj); err != nil {
		log.Fatalf("stream: %v", err)
	}

	durable := "kv-syncd-" + *nodeID
	sub, err := jsRep.PullSubscribe(*repSubj, durable, nats.BindStream(*repStream), nats.ManualAck(), nats.AckExplicit())
	if err != nil {
		log.Fatalf("pullsub: %v", err)
	}

	go func() {
		for ctx.Err() == nil {
			msgs, err := sub.Fetch(64, nats.MaxWait(2*time.Second))
			if err != nil {
				if errors.Is(err, nats.ErrTimeout) {
					continue
				}
				continue
			}
			for _, msg := range msgs {
				var op Operation
				if json.Unmarshal(msg.Data, &op) != nil {
					_ = msg.Ack()
					continue
				}
				if op.NodeID == *nodeID || op.Bucket != *bucket {
					_ = msg.Ack()
					continue
				}

				localMeta := readMeta(metaKV, op.Key)
				if !remoteWins(op.TS, op.NodeID, localMeta) {
					_ = msg.Ack()
					continue
				}

				suppress.Add(op.Key)
				switch strings.ToLower(op.Op) {
				case "put":
					if op.Value != nil {
						if _, e := dataKV.Put(op.Key, []byte(*op.Value)); e == nil {
							writeMeta(metaKV, op.Key, Meta{TS: op.TS, NodeID: op.NodeID, Tombstone: false})
						}
					}
				case "del", "delete":
					if e := dataKV.Delete(op.Key); e == nil {
						writeMeta(metaKV, op.Key, Meta{TS: op.TS, NodeID: op.NodeID, Tombstone: true})
					}
				}
				suppress.Done(op.Key)
				_ = msg.Ack()
			}
		}
	}()

	go func() {
		w, err := dataKV.WatchAll()
		if err != nil {
			log.Fatalf("watch: %v", err)
		}
		defer w.Stop()

		for {
			e := <-w.Updates()
			if e == nil {
				break
			}
		}

		log.Printf("[%s] local=%s rep=%s bucket=%s subj=%s", *nodeID, *natsURL, *repURL, *bucket, *repSubj)

		for ctx.Err() == nil {
			e := <-w.Updates()
			if e == nil {
				continue
			}
			key := e.Key()
			if suppress.Has(key) {
				continue
			}

			ts := clock.Next()
			op := Operation{Bucket: *bucket, Key: key, TS: ts, NodeID: *nodeID}

			switch e.Operation() {
			case nats.KeyValuePut:
				op.Op = "put"
				v := string(e.Value())
				op.Value = &v
				writeMeta(metaKV, key, Meta{TS: ts, NodeID: *nodeID, Tombstone: false})
			case nats.KeyValueDelete, nats.KeyValuePurge:
				op.Op = "del"
				writeMeta(metaKV, key, Meta{TS: ts, NodeID: *nodeID, Tombstone: true})
			default:
				continue
			}

			b, _ := json.Marshal(op)
			_, _ = jsRep.Publish(*repSubj, b)
		}
	}()

	if *reconEvery > 0 {
		go func() {
			t := time.NewTicker(*reconEvery)
			defer t.Stop()
			for ctx.Err() == nil {
				<-t.C

				l, err := dataKV.ListKeys()
				if err != nil {
					continue
				}
				for k := range l.Keys() {
					if strings.HasPrefix(k, "__") {
						continue
					}
					m := readMeta(metaKV, k)

					var op Operation
					op.Bucket = *bucket
					op.Key = k
					op.TS = m.TS
					op.NodeID = m.NodeID

					if m.Tombstone {
						op.Op = "del"
					} else {
						e, err := dataKV.Get(k)
						if err != nil {
							continue
						}
						op.Op = "put"
						v := string(e.Value())
						op.Value = &v
					}

					b, _ := json.Marshal(op)
					_, _ = jsRep.Publish(*repSubj, b)
				}
				_ = l.Stop()
			}
		}()
	}

	<-ctx.Done()
	fmt.Println("exit")
}
