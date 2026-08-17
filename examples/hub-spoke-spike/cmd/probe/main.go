// probe - a small listening tool for POSITIVE proof of site isolation
// (AE-051 spike). It connects to the given NATS node (unauthenticated -> the
// server's account via no_auth_user), subscribes to a subject for a given
// duration and prints the number of received messages as "received=N".
//
// Usage in the script: a probe running on spoke-A subscribed to counterB.> must
// receive 0 messages, while traffic flows on spoke-B - this proves isolation
// positively (the account did not deliver foreign traffic), not just an absence of a reply.
package main

import (
	"flag"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
)

func main() {
	url := flag.String("url", "", "NATS node (nats://host:port)")
	subject := flag.String("subject", "", "subject to listen on (may be a wildcard)")
	secs := flag.Float64("secs", 2, "listen duration in seconds")
	flag.Parse()

	if *url == "" || *subject == "" {
		log.Fatal("usage: probe --url <nats://...> --subject <subj> [--secs N]")
	}

	nc, err := nats.Connect(*url)
	if err != nil {
		log.Fatalf("connect %s: %v", *url, err)
	}
	defer nc.Close()

	var received int64
	sub, err := nc.Subscribe(*subject, func(_ *nats.Msg) {
		atomic.AddInt64(&received, 1)
	})
	if err != nil {
		log.Fatalf("subscribe %s: %v", *subject, err)
	}
	if err := nc.Flush(); err != nil {
		log.Fatalf("flush: %v", err)
	}

	time.Sleep(time.Duration(*secs * float64(time.Second)))
	_ = sub.Unsubscribe()

	fmt.Printf("received=%d\n", atomic.LoadInt64(&received))
}
