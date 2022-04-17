package main

import (
	"context"
	"log"
	"net"
	"net/http"
	"sync"
	"time"
	"yarpc"
)

type Foo int

type Args struct{ Num1, Num2 int }

func (f Foo) Sum(args Args, reply *int) error {
	*reply = args.Num1 + args.Num2
	return nil
}

func startRPCServer(addr chan string) {
	var foo Foo
	if err := yarpc.Register(&foo); nil != err {
		log.Fatal("register error:", err)
	}

	l, err := net.Listen("tcp", ":0")
	if nil != err {
		log.Fatalln("network error:", err)
	}
	log.Println("start rpc server on", l.Addr())

	addr <- l.Addr().String()
	yarpc.Accept(l)
}

func startHTTPServer(addr chan string) {
	var foo Foo
	if err := yarpc.Register(&foo); nil != err {
		log.Fatal("register error:", err)
	}

	l, err := net.Listen("tcp", ":9999")
	if nil != err {
		log.Fatalln("network error:", err)
	}
	log.Println("start rpc server on", l.Addr())
	yarpc.HandleHTTP()
	addr <- l.Addr().String()
	_ = http.Serve(l, nil)
}

func callRPC(addr chan string) {
	client, _ := yarpc.Dial("tcp", <-addr)
	defer func() { _ = client.Close() }()

	time.Sleep(time.Second)

	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			args := &Args{i, i * i}
			var reply int
			if err := client.Call(context.Background(), "Foo.Sum", args, &reply); nil != err {
				log.Fatal("call Foo.Sum error:", err)
			}

			log.Printf("%d + %d = %d", args.Num1, args.Num2, reply)
		}(i)
	}

	wg.Wait()
}

func callHTTP(addr chan string) {
	client, _ := yarpc.DialHTTP("tcp", <-addr)
	defer func() { _ = client.Close() }()

	time.Sleep(time.Second)
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			args := &Args{i, i * i}
			var reply int
			if err := client.Call(context.Background(), "Foo.Sum", args, &reply); nil != err {
				log.Fatal("call Foo.Sum error:", err)
			}

			log.Printf("%d + %d = %d", args.Num1, args.Num2, reply)
		}(i)
	}

	wg.Wait()
}

func main() {
	log.SetFlags(0)
	addr := make(chan string)
	go callHTTP(addr)
	startHTTPServer(addr)
}
