package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"time"
	"yarpc"
	"yarpc/codec"
)

func startServer(addr chan string) {
	l, err := net.Listen("tcp", ":0")
	if nil != err {
		log.Fatalln("network error:", err)
	}
	log.Println("start rpc server on", l.Addr())

	addr <- l.Addr().String()
	yarpc.Accept(l)
}

func main() {
	addr := make(chan string)
	go startServer(addr)

	conn, _ := net.Dial("tcp", <-addr)
	defer func() { _ = conn.Close() }()

	time.Sleep(time.Second)

	_ = json.NewEncoder(conn).Encode(yarpc.DefaultOption)
	cc := codec.NewGobCodec(conn)

	for i := 0; i < 5; i++ {
		h := &codec.Header{
			ServerMethod: "Foo.Bar",
			Seq:          uint64(i),
		}

		_ = cc.Write(h, fmt.Sprintf("yarpc req %d", h.Seq))
		_ = cc.ReadHeader(h)
		var reply string
		_ = cc.ReadBody(&reply)

		log.Println("reply: ", reply)
	}
}
