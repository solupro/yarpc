package main

import (
	"context"
	"log"
	"net"
	"net/http"
	"sync"
	"time"
	"yarpc"
	"yarpc/xclient"
)

type Foo int

type Args struct{ Num1, Num2 int }

func (f Foo) Sum(args Args, reply *int) error {
	*reply = args.Num1 + args.Num2
	return nil
}

func (f Foo) Sleep(args Args, reply *int) error {
	time.Sleep(time.Second * time.Duration(args.Num1))
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

func startRPCServer2(addr chan string) {
	var foo Foo
	l, err := net.Listen("tcp", ":0")
	if nil != err {
		log.Fatalln("network error:", err)
	}
	log.Println("start rpc server on", l.Addr())

	server := yarpc.NewServer()
	_ = server.Register(&foo)
	addr <- l.Addr().String()
	server.Accept(l)
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

func foo(x *xclient.XClient, ctx context.Context, typ, serviceMethod string, args *Args) {
	var reply int
	var err error
	switch typ {
	case "call":
		err = x.Call(ctx, serviceMethod, args, &reply)
	case "broadcast":
		err = x.Broadcast(ctx, serviceMethod, args, &reply)
	}

	if err != nil {
		log.Printf("%s %s error: %v", typ, serviceMethod, err)
	} else {
		log.Printf("%s %s success: %d + %d = %d", typ, serviceMethod, args.Num1, args.Num2, reply)
	}
}

func call(addr1, addr2 string) {
	d := xclient.NewMultiServersDiscovery([]string{"tcp@" + addr1, "tcp@" + addr2})
	x := xclient.NewXClient(d, xclient.RANDOM_SELECT, nil)
	defer func() { _ = x.Close() }()

	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)

		go func(i int) {
			defer wg.Done()
			foo(x, context.Background(), "call", "Foo.Sum", &Args{i, i * i})
		}(i)
	}
	wg.Wait()
}

func broadcast(addr1, addr2 string) {
	d := xclient.NewMultiServersDiscovery([]string{"tcp@" + addr1, "tcp@" + addr2})
	x := xclient.NewXClient(d, xclient.RANDOM_SELECT, nil)
	defer func() { _ = x.Close() }()

	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)

		go func(i int) {
			defer wg.Done()
			foo(x, context.Background(), "broadcast", "Foo.Sum", &Args{i, i * i})
			ctx, _ := context.WithTimeout(context.Background(), time.Second*2)
			foo(x, ctx, "broadcast", "Foo.Sleep", &Args{Num1: i, Num2: i * i})
		}(i)
	}
	wg.Wait()
}

func main() {
	log.SetFlags(0)
	//addr := make(chan string)
	//go callHTTP(addr)
	//startHTTPServer(addr)

	ch1 := make(chan string)
	ch2 := make(chan string)
	go startRPCServer2(ch1)
	go startRPCServer2(ch2)

	addr1 := <-ch1
	addr2 := <-ch2

	time.Sleep(time.Second)
	call(addr1, addr2)
	broadcast(addr1, addr2)
}
