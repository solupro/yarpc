package yarpc

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"strings"
	"sync"
	"time"
	"yarpc/codec"
)

type Call struct {
	Seq           uint64
	ServiceMethod string
	Args          interface{}
	Reply         interface{}
	Error         error
	Done          chan *Call
}

func (call *Call) done() {
	call.Done <- call
}

type Client struct {
	cc       codec.Codec
	opt      *Option
	sending  sync.Mutex // protect following
	header   codec.Header
	mu       sync.Mutex // protect following
	seq      uint64
	pending  map[uint64]*Call
	closing  bool // user has called Close
	shutdown bool // server has told us to stop
}

var _ io.Closer = (*Client)(nil)

func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closing {
		return rpc.ErrShutdown
	}
	c.closing = true
	return c.cc.Close()
}

func (c *Client) IsAvailable() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	return !c.shutdown && !c.closing
}

func (c *Client) registerCall(call *Call) (uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closing || c.shutdown {
		return 0, rpc.ErrShutdown
	}
	call.Seq = c.seq
	c.pending[call.Seq] = call
	c.seq++

	return call.Seq, nil
}

func (c *Client) removeCall(seq uint64) *Call {
	c.mu.Lock()
	defer c.mu.Unlock()

	call := c.pending[seq]
	delete(c.pending, seq)

	return call
}

func (c *Client) terminateCalls(err error) {
	c.sending.Lock()
	defer c.sending.Unlock()
	c.mu.Lock()
	defer c.mu.Unlock()

	c.shutdown = true
	for _, call := range c.pending {
		call.Error = err
		call.done()
	}
}

func (c *Client) receive() {
	var err error
	for nil == err {
		var h codec.Header
		if err = c.cc.ReadHeader(&h); nil != err {
			break
		}

		call := c.removeCall(h.Seq)
		switch {
		case nil == call:
			// it usually means that Write partially failed
			// and call was already removed.
			err = c.cc.ReadBody(nil)
		case "" != h.Error:
			call.Error = fmt.Errorf(h.Error)
			err = c.cc.ReadBody(nil)
			call.done()
		default:
			err = c.cc.ReadBody(call.Reply)
			if nil != err {
				call.Error = errors.New("reading body " + err.Error())
			}
			call.done()
		}
	}

	// error occurs, so terminateCalls pending calls
	c.terminateCalls(err)
}

func (c *Client) send(call *Call) {
	c.sending.Lock()
	defer c.sending.Unlock()

	seq, err := c.registerCall(call)
	if nil != err {
		call.Error = err
		call.done()
		return
	}

	c.header.ServerMethod = call.ServiceMethod
	c.header.Seq = seq
	c.header.Error = ""

	if err := c.cc.Write(&c.header, call.Args); err != nil {
		call := c.removeCall(seq)
		if nil != call {
			call.Error = err
			call.done()
		}
	}
}

func (c *Client) Go(serviceMethod string, args, reply interface{}, done chan *Call) *Call {
	if nil == done {
		done = make(chan *Call, 10)
	} else if cap(done) == 0 {
		log.Panic("yarpc: done channel is unbuffered")
	}

	call := &Call{
		ServiceMethod: serviceMethod,
		Args:          args,
		Reply:         reply,
		Done:          done,
	}
	c.send(call)

	return call
}

func (c *Client) Call(ctx context.Context, serviceMethod string, args, reply interface{}) error {
	call := c.Go(serviceMethod, args, reply, make(chan *Call, 1))
	select {
	case <-ctx.Done():
		c.removeCall(call.Seq)
		return errors.New("rpc client: call failed: " + ctx.Err().Error())
	case call := <-call.Done:
		return call.Error
	}
}

func NewClient(conn net.Conn, opt *Option) (*Client, error) {
	f := codec.NewCodecFuncMap[opt.CodecType]
	if nil == f {
		err := fmt.Errorf("invalid codec type %s", opt.CodecType)
		log.Println("rpc client: codec error:", err)
		return nil, err
	}

	// send option with server
	if err := json.NewEncoder(conn).Encode(opt); nil != err {
		log.Println("rpc client: options error:", err)
		_ = conn.Close()
		return nil, err
	}

	return newClientCodec(f(conn), opt), nil
}

func newClientCodec(cc codec.Codec, opt *Option) *Client {
	client := &Client{
		seq:     1,
		cc:      cc,
		opt:     opt,
		pending: make(map[uint64]*Call),
	}
	go client.receive()

	return client
}

func parseOptions(opts ...*Option) (*Option, error) {
	if len(opts) == 0 || opts[0] == nil {
		return DefaultOption, nil
	}

	if 1 != len(opts) {
		return nil, errors.New("invalid options more then 1")
	}

	opt := opts[0]
	opt.MagicNumber = DefaultOption.MagicNumber
	if "" == opt.CodecType {
		opt.CodecType = DefaultOption.CodecType
	}

	return opt, nil
}

func Dial(network, address string, opts ...*Option) (client *Client, err error) {
	opt, err := parseOptions(opts...)
	if nil != err {
		return nil, err
	}

	conn, err := net.Dial(network, address)
	if nil != err {
		return nil, err
	}
	defer func() {
		if nil == client {
			_ = conn.Close()
		}
	}()

	return NewClient(conn, opt)
}

type clientResult struct {
	client *Client
	err    error
}

type newClientFunc func(conn net.Conn, opt *Option) (client *Client, err error)

func dialTimeout(f newClientFunc, network, addr string, opts ...*Option) (client *Client, err error) {
	opt, err := parseOptions(opts...)
	if nil != err {
		return nil, err
	}

	conn, err := net.DialTimeout(network, addr, opt.ConnectTimeout)
	if nil != err {
		return nil, err
	}
	defer func() {
		if nil != err {
			_ = conn.Close()
		}
	}()

	ch := make(chan clientResult)
	go func() {
		client, err := f(conn, opt)
		ch <- clientResult{client, err}
	}()

	if 0 == opt.ConnectTimeout {
		result := <-ch
		return result.client, result.err
	}

	// wait for timeout
	select {
	case <-time.After(opt.ConnectTimeout):
		return nil, fmt.Errorf("rpc client: connect timeout: expect within %s", opt.ConnectTimeout)
	case rs := <-ch:
		return rs.client, rs.err
	}
}

func NewHTTPClient(conn net.Conn, opt *Option) (*Client, error) {
	_, _ = io.WriteString(conn, fmt.Sprintf("CONNECT %s HTTP/1.0\n\n", DEFAULT_RPC_PATH))

	// Require successful HTTP response
	// before switching to RPC protocol.
	resp, err := http.ReadResponse(bufio.NewReader(conn), &http.Request{Method: http.MethodConnect})
	if nil == err && resp.Status == CONNECTED {
		return NewClient(conn, opt)
	}

	if nil == err {
		err = errors.New("unexpected HTTP response: " + resp.Status)
	}
	return nil, err
}

func DialHTTP(network, address string, opts ...*Option) (*Client, error) {
	return dialTimeout(NewHTTPClient, network, address, opts...)
}

// XDial calls different functions to connect to a RPC server
// according the first parameter rpcAddr.
// rpcAddr is a general format (protocol@addr) to represent a rpc server
// eg, http@10.0.0.1:7001, tcp@10.0.0.1:9999, unix@/tmp/geerpc.sock
func XDial(rpcAddr string, opts ...*Option) (*Client, error) {
	parts := strings.Split(rpcAddr, "@")
	if len(parts) != 2 {
		return nil, fmt.Errorf("rpc client err: wrong format '%s', expect protocol@addr", rpcAddr)
	}
	protocol, addr := parts[0], parts[1]
	switch protocol {
	case "http":
		return DialHTTP("tcp", addr, opts...)
	default:
		// tcp, unix or other transport protocol
		return Dial(protocol, addr, opts...)
	}
}
