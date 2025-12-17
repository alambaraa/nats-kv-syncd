package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/nats-io/nats.go"
)

func main() {
	natsURL := flag.String("nats-url", "nats://localhost:4222", "NATS URL")
	bucket := flag.String("bucket", "config", "KV bucket")
	nodeID := flag.String("node-id", "", "Node ID")
	_ = flag.String("rep-subj", "rep.kv.ops", "Replication subject")
	flag.Parse()

	if *nodeID == "" {
		log.Fatal("Falta --node-id")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	nc, err := nats.Connect(*natsURL)
	if err != nil {
		log.Fatalf("nats.Connect: %v", err)
	}
	defer nc.Drain()

	js, err := nc.JetStream()
	if err != nil {
		log.Fatalf("JetStream: %v", err)
	}

	kv, err := js.KeyValue(*bucket)
	if err != nil {
		log.Fatalf("KeyValue(%s): %v", *bucket, err)
	}

	w, err := kv.WatchAll()
	if err != nil {
		log.Fatalf("WatchAll: %v", err)
	}
	defer w.Stop()

	for {
		e := <-w.Updates()
		if e == nil {
			break
		}
	}

	log.Printf("[%s] Watch activo en %s (%s)", *nodeID, *bucket, *natsURL)

	for {
		select {
		case <-ctx.Done():
			return
		case e := <-w.Updates():
			if e == nil {
				continue
			}
			fmt.Printf("[%s] Cambio local: %s = %s (op=%v)\n", *nodeID, e.Key(), string(e.Value()), e.Operation())
		}
	}
}
