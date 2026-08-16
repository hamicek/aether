// probe - maly odposlouchavaci nastroj pro POZITIVNI dukaz izolace sajt
// (AE-051 spike). Pripoji se na dany NATS uzel (neautentizovane -> account
// daneho serveru pres no_auth_user), odebira subjekt po zadanou dobu a vypise
// pocet prijatych zprav jako "received=N".
//
// Pouziti ve skriptu: probe bezici na spoke-A a odebirajici counterB.> musi
// prijmout 0 zprav, zatimco na spoke-B tece provoz - tim je izolace dolozena
// pozitivne (account nedorucil cizi provoz), ne jen absenci odpovedi.
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
	url := flag.String("url", "", "NATS uzel (nats://host:port)")
	subject := flag.String("subject", "", "subjekt k odposlechu (muze byt wildcard)")
	secs := flag.Float64("secs", 2, "doba odposlechu v sekundach")
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
