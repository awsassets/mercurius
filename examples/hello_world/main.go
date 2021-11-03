package main

import (
	"flag"
	"fmt"
	"github.com/nathanieltornow/mercurius"
	"github.com/nathanieltornow/mercurius/node"
	"log"
	"strings"
	"time"
)

var (
	IP      = flag.String("ip", "", "")
	peerIPs = flag.String("peers", "", "")
	id      = flag.Int("id", 0, "")
	ok      = flag.Bool("ok", false, "")
)

type HelloWorldApp struct {
}

func (h *HelloWorldApp) Prepare(sn uint64, key uint32, content string) error {
	fmt.Println("Prepare:", sn, key, content)
	return nil
}

func (h *HelloWorldApp) Commit(sn uint64, key uint32) error {
	fmt.Println("Commit:", sn, key)

	return nil

}

func (h *HelloWorldApp) Acknowledge(sn uint64, key uint32) error {
	fmt.Println("Acknowledge:", sn, key)
	return nil
}

func main() {
	flag.Parse()
	app := &HelloWorldApp{}
	n := node.NewNode(uint32(*id), app)
	//time.Sleep(10 * time.Second)
	var peers []string
	if *peerIPs != "" {
		peers = strings.Split(*peerIPs, ",")
	} else {
		peers = make([]string, 0)
	}

	go func() {
		err := n.Start(*IP, peers)
		if err != nil {
			log.Fatalln(err)
		}
	}()

	for i := 0; i < 20; i++ {
		time.Sleep(time.Second)
		if *ok {
			n.MakeCommitRequest(&mercurius.CommitRequest{Content: "Hello World", Key: 1})
		}
	}

	time.Sleep(90 * time.Second)
}
