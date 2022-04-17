package yarpc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"yarpc/codec"
)

const MAGIC_NUMBER = 0xCAFE

const (
	CONNECTED          = "200 Connected to YARPC"
	DEFAULT_RPC_PATH   = "/_yarpc_"
	DEFAULT_DEBUG_PATH = "/debug/yarpc"
)

type Option struct {
	MagicNumber    int
	CodecType      codec.Type
	ConnectTimeout time.Duration // 0 unlimit
	HandleTimeout  time.Duration
}

var DefaultOption = &Option{
	MagicNumber: MAGIC_NUMBER,
	CodecType:   codec.GOB_TYPE,
}

type Server struct {
	serviceMap sync.Map
}

func (svr *Server) Register(rcvr interface{}) error {
	s := newService(rcvr)
	if _, dup := svr.serviceMap.LoadOrStore(s.name, s); dup {
		return errors.New("rpc: service already defined: " + s.name)
	}
	return nil
}

func (svr *Server) findService(serviceMethod string) (svc *service, mtype *methodType, err error) {
	dot := strings.LastIndex(serviceMethod, ".")
	if dot < 0 {
		err = errors.New("rpc server: service/method request ill-formed: " + serviceMethod)
		return
	}

	serviceName, methodName := serviceMethod[:dot], serviceMethod[dot+1:]
	s, ok := svr.serviceMap.Load(serviceName)
	if !ok {
		err = errors.New("rpc server: can't find service " + serviceName)
		return
	}

	svc = s.(*service)
	mtype = svc.method[methodName]
	if nil == mtype {
		err = errors.New("rpc server: can't find method " + methodName)
	}

	return
}

func (svr *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if http.MethodConnect != r.Method {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusMethodNotAllowed)
		_, _ = io.WriteString(w, "405 must CONNECT\n")
		return
	}

	conn, _, err := w.(http.Hijacker).Hijack()
	if nil != err {
		log.Printf("rpc hijacking", r.RemoteAddr, ":", err.Error())
		return
	}

	_, _ = io.WriteString(conn, "HTTP/1.0 "+CONNECTED+"\n\n")
	svr.ServeConn(conn)
}

func (svr *Server) handleHTTP() {
	http.Handle(DEFAULT_RPC_PATH, svr)
	http.Handle(DEFAULT_DEBUG_PATH, &debugHTTP{svr})
	log.Println("rpc server debug path:", DEFAULT_DEBUG_PATH)
}

func HandleHTTP() {
	DefaultServer.handleHTTP()
}

func Register(rcvr interface{}) error {
	return DefaultServer.Register(rcvr)
}

func NewServer() *Server {
	return &Server{}
}

var DefaultServer = NewServer()

func Accept(lis net.Listener) {
	DefaultServer.Accept(lis)
}

func (s *Server) Accept(lis net.Listener) {
	for {
		conn, err := lis.Accept()
		if nil != err {
			log.Println("rpc server: accept error:", err)
			return
		}

		go s.ServeConn(conn)
	}
}

//| Option{MagicNumber: xxx, CodecType: xxx} | Header{ServiceMethod ...} | Body interface{} |
//| <------      固定 JSON 编码      ------>  | <-------   编码方式由 CodeType 决定   ------->|

// | Option | Header1 | Body1 | Header2 | Body2 | ...
func (s *Server) ServeConn(conn io.ReadWriteCloser) {
	defer func() {
		_ = conn.Close()
	}()

	var opt Option
	if err := json.NewDecoder(conn).Decode(&opt); nil != err {
		log.Println("rpc server: options error: ", err)
		return
	}

	if MAGIC_NUMBER != opt.MagicNumber {
		log.Printf("rpc server: invalid magic number %x", opt.MagicNumber)
		return
	}

	f := codec.NewCodecFuncMap[opt.CodecType]
	if nil == f {
		log.Printf("rpc server: invalid codec type %s", opt.CodecType)
		return
	}

	s.serveCodec(f(conn), &opt)
}

// invalidRequest is a placeholder for response argv when error occurs
var invalidRequest = struct{}{}

func (s *Server) serveCodec(cc codec.Codec, opt *Option) {
	sending := new(sync.Mutex) // make sure to send a complete response
	wg := new(sync.WaitGroup)  // wait until all request are handled

	for {
		req, err := s.readRequest(cc)
		if nil != err {
			if nil == req {
				break // it's not possible to recover, so close the connection
			}

			req.h.Error = err.Error()
			s.sendResponse(cc, req.h, invalidRequest, sending)
			continue
		}

		wg.Add(1)
		go s.handleRequest(cc, req, sending, wg, opt.HandleTimeout)
	}
	wg.Wait()
	_ = cc.Close()
}

type request struct {
	h            *codec.Header
	argv, replyv reflect.Value
	mtype        *methodType
	svc          *service
}

func (s *Server) readRequestHeader(cc codec.Codec) (*codec.Header, error) {
	var h codec.Header
	if err := cc.ReadHeader(&h); nil != err {
		if err != io.EOF && err != io.ErrUnexpectedEOF {
			log.Println("rpc server: read header error:", err)
		}
		return nil, err
	}

	return &h, nil
}

func (s *Server) readRequest(cc codec.Codec) (*request, error) {
	h, err := s.readRequestHeader(cc)
	if nil != err {
		return nil, err
	}

	req := &request{h: h}
	req.svc, req.mtype, err = s.findService(h.ServerMethod)
	if nil != err {
		return req, err
	}
	req.argv = req.mtype.newArgv()
	req.replyv = req.mtype.newReplyv()

	// make sure that argvi is a pointer, ReadBody need a pointer as parameter
	argvi := req.argv.Interface()
	if req.argv.Type().Kind() != reflect.Ptr {
		argvi = req.argv.Addr().Interface()
	}

	if err = cc.ReadBody(argvi); nil != err {
		log.Println("rpc server: read argv err:", err)
		return req, err
	}

	return req, nil
}

func (s *Server) sendResponse(cc codec.Codec, h *codec.Header, body interface{}, sending *sync.Mutex) {
	sending.Lock()
	defer sending.Unlock()

	if err := cc.Write(h, body); nil != err {
		log.Println("rpc server: write response error:", err)
	}
}

func (s *Server) handleRequest(cc codec.Codec, req *request, sending *sync.Mutex, wg *sync.WaitGroup, timeout time.Duration) {
	defer wg.Done()

	called := make(chan struct{})
	sent := make(chan struct{})
	ctx, cannel := context.WithCancel(context.Background())
	defer cannel()

	go func(ctx context.Context) {
		// 执行方法
		err := req.svc.call(req.mtype, req.argv, req.replyv)
		// TODO timeout后是否会阻塞导致协程不释放？
		select {
		case <-ctx.Done():
			log.Println("time out, cancel execute")
			return
		case called <- struct{}{}:
		}
		if nil != err {
			req.h.Error = err.Error()
			s.sendResponse(cc, req.h, invalidRequest, sending)
			sent <- struct{}{}
			return
		}
		s.sendResponse(cc, req.h, req.replyv.Interface(), sending)
		sent <- struct{}{}
	}(ctx)

	if 0 == timeout {
		<-called
		<-sent
		return
	}

	select {
	case <-time.After(timeout):
		req.h.Error = fmt.Sprintf("rpc server: request handle timeout: expect within %s", timeout)
		s.sendResponse(cc, req.h, invalidRequest, sending)
	case <-called:
		<-sent
	}
}

type methodType struct {
	method    reflect.Method
	ArgType   reflect.Type
	ReplyType reflect.Type
	numCalls  uint64
}

func (m *methodType) NumCalls() uint64 {
	return atomic.LoadUint64(&m.numCalls)
}

func (m *methodType) newArgv() reflect.Value {
	var argv reflect.Value
	// 区分指针和值类型
	if m.ArgType.Kind() == reflect.Ptr {
		argv = reflect.New(m.ArgType.Elem())
	} else {
		argv = reflect.New(m.ArgType).Elem()
	}

	return argv
}

func (m *methodType) newReplyv() reflect.Value {
	// 返回值必须是指针
	replyv := reflect.New(m.ReplyType.Elem())
	switch m.ReplyType.Elem().Kind() {
	case reflect.Map:
		replyv.Elem().Set(reflect.MakeMap(m.ReplyType.Elem()))
	case reflect.Slice:
		replyv.Elem().Set(reflect.MakeSlice(m.ReplyType.Elem(), 0, 0))
	}

	return replyv
}
