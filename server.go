// Copyright 2015 Giulio Iotti. All rights reserved.
// 使用此源代码受MIT风格许可证约束
// 许可证可在LICENSE文件中找到。

package pingo

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"io"
	"net"
	"net/rpc"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"strings"
)

// 注册插件导出的新对象。该对象必须是
// 一个导出的符号，并遵守标准
// "rpc"模块中对象必须遵守的所有规则。
//
// 如果在Run之后调用，Register将会panic。
func Register(obj interface{}) {
	if defaultServer.running {
		panic("不要在Run之后调用Register")
	}
	defaultServer.register(obj)
}

// Run将启动使插件可用所需的所有步骤。
func Run() error {
	if !flag.Parsed() {
		flag.Parse()
	}
	return defaultServer.run()
}

// 用于插件控制的内部对象
type PingoRpc struct{}

// 内部对象的默认构造函数。请勿手动调用。
func NewPingoRpc() *PingoRpc {
	return &PingoRpc{}
}

// 关闭插件的内部RPC调用。请勿手动调用。
func (s *PingoRpc) Exit(status int, unused *int) error {
	os.Exit(status)
	return nil
}

type config struct {
	proto   string
	addr    string
	prefix  string
	unixdir string
}

func makeConfig() *config {
	c := &config{}
	flag.StringVar(&c.proto, "pingo:proto", "unix", "使用的协议：unix或tcp")
	flag.StringVar(&c.unixdir, "pingo:unixdir", "", "unix套接字的替代目录")
	flag.StringVar(&c.prefix, "pingo:prefix", "pingo", "输出行的前缀")
	return c
}

type rpcServer struct {
	*rpc.Server
	secret  string
	objs    []string
	conf    *config
	running bool
}

func newRpcServer() *rpcServer {
	r := &rpcServer{
		Server: rpc.NewServer(),
		secret: randstr(64),
		objs:   make([]string, 0),
		conf:   makeConfig(), // conf在此点之后保持不变
	}
	r.register(&PingoRpc{})
	return r
}

var defaultServer = newRpcServer()

// bufReadWriteCloser 实现了一个带缓冲的读写关闭器
// 组合了带缓冲的读取功能和底层的读写关闭能力
type bufReadWriteCloser struct {
	*bufio.Reader
	r io.ReadWriteCloser
}

// newBufReadWriteCloser 创建一个新的带缓冲读写关闭器
func newBufReadWriteCloser(r io.ReadWriteCloser) *bufReadWriteCloser {
	return &bufReadWriteCloser{
		Reader: bufio.NewReader(r),
		r:      r,
	}
}

// Write 直接将数据写入底层读写关闭器
func (b *bufReadWriteCloser) Write(data []byte) (int, error) {
	return b.r.Write(data)
}

// Close 关闭底层读写关闭器
func (b *bufReadWriteCloser) Close() error {
	return b.r.Close()
}

// readHeaders 读取HTTP风格的头部数据，直到遇到空行
// 返回包含所有头部的字节切片
func readHeaders(brwc *bufReadWriteCloser) ([]byte, error) {
	var buf bytes.Buffer
	var consecutiveNewlines int

	// 读取直到遇到连续两个换行符（空行）
	for {
		b, err := brwc.ReadByte()
		if err != nil {
			return nil, err
		}

		buf.WriteByte(b)

		if b == '\n' {
			consecutiveNewlines++
			if consecutiveNewlines == 2 {
				break
			}
		} else {
			consecutiveNewlines = 0
		}
	}

	return buf.Bytes(), nil
}

// parseHeaders 解析HTTP风格的头部并将其存储到提供的映射中
// 从缓冲读取器中读取头部，并按键值对存入map
func parseHeaders(brwc *bufReadWriteCloser, m map[string]string) error {
	headers, err := readHeaders(brwc)
	if err != nil {
		return err
	}

	r := bytes.NewReader(headers)
	scanner := bufio.NewScanner(r)

	for scanner.Scan() {
		line := scanner.Text()
		// 跳过空行
		if line == "" {
			continue
		}

		// 分割"键: 值"格式的行
		parts := strings.SplitN(line, ": ", 2)
		if len(parts) != 2 || parts[0] == "" {
			continue
		}
		m[parts[0]] = parts[1]
	}

	return scanner.Err()
}

// authConn 验证连接的认证令牌是否有效
// 通过比较提供的令牌与服务器的密钥来验证连接
func (r *rpcServer) authConn(token string) bool {
	return token != "" && token == r.secret
}

// serveConn 处理传入的连接请求
// 读取头部信息，验证认证令牌，并将连接交给RPC服务器处理
func (r *rpcServer) serveConn(conn io.ReadWriteCloser, h msgChannel) {
	bconn := newBufReadWriteCloser(conn)
	defer bconn.Close()

	// 读取并解析HTTP风格的头部
	headers := make(map[string]string)
	if err := parseHeaders(bconn, headers); err != nil {
		h.output("error", err.Error())
		return
	}

	if !r.authConn(headers["Auth-Token"]) {
		h.output("error", "认证失败")
		return
	}

	// 只有通过认证的连接才能被处理
	r.Server.ServeConn(bconn)
}

// register 向RPC服务器注册一个对象
// 获取对象的类型名称并添加到对象列表中
func (r *rpcServer) register(obj interface{}) {
	element := reflect.TypeOf(obj).Elem()
	r.objs = append(r.objs, element.Name())
	r.Server.Register(obj)
}

// connection 定义了连接管理接口
// 包含获取地址和重试次数的方法
type connection interface {
	addr() string // 获取连接地址
	retries() int // 获取连接重试次数
}

// tcp 实现了基于TCP协议的连接
type tcp int

// addr 返回TCP连接的地址字符串
func (t *tcp) addr() string {
	if *t < 1024 {
		// 只使用非特权端口
		*t = 1023
	}

	*t = *t + 1
	return fmt.Sprintf("127.0.0.1:%d", *t)
}

// retries 返回TCP连接的最大重试次数
func (t *tcp) retries() int {
	return 500
}

// unix 实现了基于UNIX套接字的连接
type unix string

// addr 返回UNIX套接字的路径
func (u *unix) addr() string {
	name := randstr(8)
	if *u != "" {
		name = filepath.FromSlash(path.Join(string(*u), name))
	}
	return name
}

// retries 返回UNIX套接字连接的最大重试次数
func (u *unix) retries() int {
	return 4
}

// run 启动RPC服务器并监听连接
// 根据配置创建连接，并在成功建立连接后开始接受请求
func (r *rpcServer) run() error {
	var conn connection
	var err error
	var listener net.Listener

	// 标记服务器为运行状态
	r.running = true

	// 创建元数据处理器并输出可用对象列表
	h := msgChannel(r.conf.prefix)
	h.output("objects", strings.Join(r.objs, ", "))

	// 根据配置选择连接类型
	switch r.conf.proto {
	case "tcp":
		conn = new(tcp)
	default:
		r.conf.proto = "unix"
		conn = new(unix)
	}

	// 尝试监听，最多重试指定次数
	for i := 0; i < conn.retries(); i++ {
		r.conf.addr = conn.addr()
		listener, err = net.Listen(r.conf.proto, r.conf.addr)
		if err == nil {
			break
		}
	}

	// 如果所有尝试都失败，返回错误
	if err != nil {
		h.output("fatal", fmt.Sprintf("%s: 无法在%d次尝试中连接，使用%s协议",
			errorCodeConnFailed, conn.retries(), r.conf.proto))
		return err
	}

	// 输出认证令牌和就绪状态
	h.output("auth-token", defaultServer.secret)
	h.output("ready", fmt.Sprintf("proto=%s addr=%s", r.conf.proto, r.conf.addr))

	// 开始接受连接并处理请求
	for {
		// 接受连接
		conn, err := listener.Accept()
		if err != nil {
			h.output("fatal", fmt.Sprintf("err-http-serve: %s", err.Error()))
			continue
		}

		// 处理连接
		go r.serveConn(conn, h)
	}
}
