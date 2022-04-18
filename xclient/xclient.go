package xclient

import (
	"context"
	"io"
	"reflect"
	"sync"
	. "yarpc"
)

type XClient struct {
	d       Discovery
	mode    SelectMode
	opt     *Option
	mu      sync.Mutex
	clients map[string]*Client
}

func NewXClient(d Discovery, mode SelectMode, opt *Option) *XClient {
	return &XClient{d: d, mode: mode, opt: opt, clients: make(map[string]*Client)}
}

var _ io.Closer = (*XClient)(nil)

func (x *XClient) Close() error {
	x.mu.Lock()
	defer x.mu.Unlock()

	for key, client := range x.clients {
		_ = client.Close()
		delete(x.clients, key)
	}

	return nil
}

func (x *XClient) dial(rpcAddr string) (*Client, error) {
	x.mu.Lock()
	defer x.mu.Unlock()

	client, ok := x.clients[rpcAddr]
	if ok && !client.IsAvailable() {
		_ = client.Close()
		delete(x.clients, rpcAddr)
		client = nil
	}

	if nil == client {
		var err error
		client, err = XDial(rpcAddr, x.opt)
		if nil != err {
			return nil, err
		}
		x.clients[rpcAddr] = client
	}

	return client, nil
}

func (x *XClient) call(rpcAddr string, ctx context.Context, serviceMethod string, args, reply interface{}) error {
	client, err := x.dial(rpcAddr)
	if nil != err {
		return err
	}

	return client.Call(ctx, serviceMethod, args, reply)
}

func (x *XClient) Call(ctx context.Context, serviceMethod string, args, reply interface{}) error {
	rpcAddr, err := x.d.Get(x.mode)
	if nil != err {
		return err
	}

	return x.call(rpcAddr, ctx, serviceMethod, args, reply)
}

func (x *XClient) Broadcast(ctx context.Context, serviceMethod string, args, reply interface{}) error {
	servers, err := x.d.GetAll()
	if nil != err {
		return err
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	var e error
	replayDone := reply == nil // if reply is nil, don't need to set value
	ctx, cancel := context.WithCancel(ctx)

	for _, rpcAddr := range servers {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()

			var cloneReplay interface{}
			if nil != reply {
				cloneReplay = reflect.New(reflect.ValueOf(reply).Elem().Type()).Interface()
			}

			err := x.call(addr, ctx, serviceMethod, args, cloneReplay)
			mu.Lock()
			if nil != err && nil == e {
				e = err
				cancel()
			}
			if nil == err && !replayDone {
				reflect.ValueOf(reply).Elem().Set(reflect.ValueOf(cloneReplay).Elem())
				replayDone = true
			}
			mu.Unlock()
		}(rpcAddr)
	}
	wg.Wait()

	return e
}
