package plugo

import (
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/rpc"
	"plugo/config"
	"plugo/helper"
	"reflect"
	"strings"
	"time"
)

var defaultServer = newRpcServer()

// rpcServer is the internal server for the plugin.
type rpcServer struct {
	*rpc.Server
	secret  string
	objs    []string
	conf    *config.Config
	running bool
}

// newRpcServer creates a new rpcServer.
func newRpcServer() *rpcServer {
	rand.NewSource(time.Now().UTC().UnixNano())
	r := &rpcServer{
		Server: rpc.NewServer(),
		secret: helper.Randstr(64),
		objs:   make([]string, 0),
		conf:   config.MakeConfig(), // conf remains fixed after this point
	}
	r.register(&config.PlugoRpc{})
	return r
}

// authConn checks if the connection is authenticated.
func (r *rpcServer) authConn(token string) bool {
	if token != "" && token == r.secret {
		return true
	}
	return false
}

// serveConn serves a connection.
func (r *rpcServer) serveConn(conn io.ReadWriteCloser, _ helper.Meta) {
	bconn := helper.NewBufReadWriteCloser(conn)
	defer bconn.Close()

	headers := make(map[string]string)
	if err := helper.ParseHeaders(bconn, headers); err != nil {
		return
	}

	if r.authConn(headers["Auth-Token"]) {
		r.Server.ServeConn(bconn)
	}
}

// register registers an object with the rpcServer.
func (r *rpcServer) register(obj interface{}) {
	element := reflect.TypeOf(obj).Elem()
	r.objs = append(r.objs, element.Name())
	r.Server.Register(obj)
}

// run runs the rpcServer.
func (r *rpcServer) run() error {
	var conn connection
	var err error
	var listener net.Listener

	r.running = true

	h := helper.Meta(r.conf.Prefix)

	// Output the objects that the plugin supports.
	h.Output("objects", strings.Join(r.objs, ", "))

	switch r.conf.Proto {
	case "tcp":
		conn = new(tcp)
	default:
		r.conf.Proto = "unix"
		conn = new(unix)
	}

	for i := 0; i < conn.retries(); i++ {
		r.conf.Addr = conn.addr()
		listener, err = net.Listen(r.conf.Proto, r.conf.Addr)
		if err == nil {
			break
		}
	}

	if err != nil {
		h.Output("fatal", fmt.Sprintf("%s: Could not connect in %d attemps, using %s protocol", config.ErrorCodeConnFailed, conn.retries(), r.conf.Proto))
		return err
	}

	// Output the auth token and the ready message.
	h.Output("auth-token", defaultServer.secret)
	// Output the ready message.
	h.Output("ready", fmt.Sprintf("proto=%s addr=%s", r.conf.Proto, r.conf.Addr))

	for {
		var conn net.Conn
		conn, err = listener.Accept()
		if err != nil {
			h.Output("fatal", fmt.Sprintf("err-http-serve: %s", err.Error()))
			continue
		}
		go r.serveConn(conn, h)
	}
}
