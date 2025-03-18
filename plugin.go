// Copyright 2015 Giulio Iotti. All rights reserved.
// 使用此源代码受MIT风格许可证约束
// 许可证可在LICENSE文件中找到。

// Package pingo 实现了创建和运行子进程作为插件的基础功能。
// 子进程将通过TCP或Unix套接字进行通信，实现一个模仿标准RPC包的接口。
package pingo

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/rpc"
	"os"
	"os/exec"
	"strings"
	"time"
)

// -----------------------------------------------------------------------------
// 错误定义
// -----------------------------------------------------------------------------

var (
	errInvalidMessage      = ErrInvalidMessage(errors.New("无效的就绪消息"))
	errRegistrationTimeout = ErrRegistrationTimeout(errors.New("注册超时"))
)

// -----------------------------------------------------------------------------
// 插件类型定义
// -----------------------------------------------------------------------------

// Plugin 表示一个插件。创建后，插件尚未启动或准备运行。
//
// 可以在初始化后设置额外的配置（如ErrorHandler和Timeout）。
//
// 使用Start()使插件可用。
type Plugin struct {
	exe         string        // 可执行文件路径
	proto       string        // 通信协议（unix或tcp）
	unixdir     string        // unix套接字目录
	params      []string      // 命令行参数
	initTimeout time.Duration // 初始化超时时间
	exitTimeout time.Duration // 退出超时时间
	handler     ErrorHandler  // 错误处理器
	running     bool          // 运行状态标志
	meta        meta          // 元数据前缀
	objsCh      chan *objects // 对象请求通道
	connCh      chan *conn    // 连接请求通道
	killCh      chan *waiter  // 终止请求通道
	exitCh      chan struct{} // 退出通知通道
}

// NewPlugin 创建一个准备启动的新插件，如果初始设置失败则返回错误。
//
// 第一个参数指定协议。可以设置为"unix"以在临时本地套接字上通信，
// 或设置为"tcp"以在本地主机上进行网络通信（使用随机的非特权端口）。
//
// 如果proto参数既不是"unix"也不是"tcp"，此构造函数将会panic。
//
// 插件可执行文件的路径应该是绝对路径。接受标准库中"exec"包接受的任何路径，
// 并且应用相同的执行规则。
//
// 可选地，可以将一些参数传递给插件可执行文件。
func NewPlugin(proto, path string, params ...string) *Plugin {
	if proto != "unix" && proto != "tcp" {
		panic("无效的协议。请指定'unix'或'tcp'。")
	}
	p := &Plugin{
		exe:         path,
		proto:       proto,
		params:      params,
		initTimeout: 20 * time.Second,
		exitTimeout: 2 * time.Second,
		handler:     NewDefaultErrorHandler(),
		meta:        meta("pingo" + randstr(5)),
		objsCh:      make(chan *objects),
		connCh:      make(chan *conn),
		killCh:      make(chan *waiter),
		exitCh:      make(chan struct{}),
	}
	return p
}

// -----------------------------------------------------------------------------
// 插件配置方法
// -----------------------------------------------------------------------------

// SetErrorHandler 设置错误（和输出）处理器实现。使用此方法设置自定义实现。
// 默认情况下，使用标准日志记录。参见ErrorHandler。
//
// 如果在Start之后调用，将会panic。
func (p *Plugin) SetErrorHandler(h ErrorHandler) {
	if p.running {
		panic("不能在Start之后调用SetErrorHandler")
	}
	p.handler = h
}

// SetTimeout 设置插件启动和关闭的最大允许时间。不允许空超时（零），
// 将使用默认值。
//
// 默认为两秒。
//
// 如果在Start之后调用，将会panic。
func (p *Plugin) SetTimeout(t time.Duration) {
	if p.running {
		panic("不能在Start之后调用SetTimeout")
	}
	if t == 0 {
		return
	}
	p.initTimeout = t
	p.exitTimeout = t
}

// SetSocketDirectory 设置Unix套接字的目录。
//
// 如果在Start之后调用，将会panic。
func (p *Plugin) SetSocketDirectory(dir string) {
	if p.running {
		panic("不能在Start之后调用SetSocketDirectory")
	}
	p.unixdir = dir
}

// String 返回插件的默认字符串表示。
func (p *Plugin) String() string {
	return fmt.Sprintf("%s %s", p.exe, strings.Join(p.params, " "))
}

// -----------------------------------------------------------------------------
// 插件控制方法
// -----------------------------------------------------------------------------

// Start 将插件作为子进程执行。Start将立即返回。对插件的任何首次调用
// 将会揭示初始化过程中可能发生的错误。
//
// Start之后的调用将挂起，直到插件已正确初始化。
func (p *Plugin) Start() {
	p.running = true
	go p.run()
}

// Stop 尝试干净地停止或终止正在运行的插件，然后释放所有资源。
// Stop在插件关闭且相关例程退出后返回。
func (p *Plugin) Stop() {
	wr := newWaiter()
	p.killCh <- wr
	wr.wait()
	p.exitCh <- struct{}{}
}

// Call 执行对插件的RPC调用。在调用Call之前，必须通过调用Start初始化插件。
//
// 如果插件尚未初始化，Call将挂起；如果在执行调用或通过Start初始化插件期间
// 发生任何错误，它将返回该错误。
//
// 有关此函数语义的更多信息，请参阅标准库中的"rpc"包。
func (p *Plugin) Call(name string, args interface{}, resp interface{}) error {
	conn := &conn{wr: newWaiter()}
	p.connCh <- conn
	conn.wr.wait()

	if conn.err != nil {
		return conn.err
	}

	return conn.client.Call(name, args, resp)
}

// Objects 返回插件中导出对象的列表。内部使用的导出对象不会被报告。
//
// 与Call类似，如果在Start之后调用，Objects将返回初始化过程中发生的任何错误。
func (p *Plugin) Objects() ([]string, error) {
	objects := &objects{wr: newWaiter()}
	p.objsCh <- objects
	objects.wr.wait()

	return objects.list, objects.err
}

// -----------------------------------------------------------------------------
// 错误处理接口和实现
// -----------------------------------------------------------------------------

// ErrorHandler 是Plugin用于报告非致命错误和插件的任何其他输出的接口。
//
// 如果在插件创建时未指定，则提供并使用默认实现。
type ErrorHandler interface {
	// Error 在插件子进程中发生非致命错误时调用。
	Error(error)
	// Print 为从插件子进程接收的每行输出调用。
	Print(interface{})
}

// DefaultErrorHandler 是默认错误处理器实现。使用Go标准库中的默认日志功能。
type DefaultErrorHandler struct{}

// NewDefaultErrorHandler 是默认错误处理器的构造函数。
func NewDefaultErrorHandler() *DefaultErrorHandler {
	return &DefaultErrorHandler{}
}

// Error 通过默认标准库设施记录日志，前置"error: "字符串。
func (e *DefaultErrorHandler) Error(err error) {
	log.Print("error: ", err)
}

// Print 通过默认标准库设施记录日志。
func (e *DefaultErrorHandler) Print(s interface{}) {
	log.Print(s)
}

// -----------------------------------------------------------------------------
// 内部常量和类型
// -----------------------------------------------------------------------------

// 内部对象名称常量
const internalObject = "PingoRpc"

// conn 表示一个连接请求
type conn struct {
	client *rpc.Client // RPC客户端
	err    error       // 错误信息
	wr     *waiter     // 等待通知
}

// waiter 实现一个简单的等待机制
type waiter struct {
	c chan struct{} // 通知通道
}

// newWaiter 创建一个新的等待器
func newWaiter() *waiter {
	return &waiter{c: make(chan struct{})}
}

// wait 等待通知完成
func (wr *waiter) wait() {
	<-wr.c
}

// done 关闭通道，表示操作完成
func (wr *waiter) done() {
	close(wr.c)
}

// reset 重置等待器状态
func (wr *waiter) reset() {
	wr.c = make(chan struct{})
}

// client 包装rpc.Client，增加认证功能
type client struct {
	*rpc.Client        // 内嵌RPC客户端
	secret      string // 认证密钥
}

// newClient 创建一个新的客户端实例
func newClient(s string, conn io.ReadWriteCloser) *client {
	return &client{secret: s, Client: rpc.NewClient(conn)}
}

// authenticate 向服务器发送认证令牌
func (c *client) authenticate(w io.Writer) error {
	_, err := io.WriteString(w, "Auth-Token: "+c.secret+"\n\n")
	return err
}

// dialAuthRpc 建立一个带认证的RPC连接
func dialAuthRpc(secret, network, address string, timeout time.Duration) (*rpc.Client, error) {
	conn, err := net.DialTimeout(network, address, timeout)
	if err != nil {
		return nil, err
	}
	c := newClient(secret, conn)
	if err := c.authenticate(conn); err != nil {
		return nil, err
	}
	return c.Client, nil
}

// objects 表示一个对象列表请求
type objects struct {
	list []string // 对象列表
	err  error    // 错误信息
	wr   *waiter  // 等待通知
}

// -----------------------------------------------------------------------------
// 控制器实现
// -----------------------------------------------------------------------------

// ctrl 控制插件的生命周期和通信
type ctrl struct {
	p           *Plugin          // 关联的插件
	objs        []string         // 对象列表
	proto, addr string           // RPC的协议和地址
	secret      string           // 连接到服务器所需的密钥
	err         error            // 不可恢复的错误，在发生后用作对调用的响应
	connCh      chan *conn       // p.connCh的别名，允许在可以处理调用时处理它们
	objsCh      chan *objects    // 同上，但用于对象请求
	timeoutCh   <-chan time.Time // 插件启动时间的超时
	waitCh      chan error       // 从子进程的Wait获取通知
	linesCh     chan string      // 从子进程获取输出行
	over        *waiter          // 响应等待此邮件循环退出的例程
	proc        *os.Process      // 可执行文件进程
	client      *rpc.Client      // 到子进程的RPC客户端
}

// newCtrl 创建新的控制器实例
func newCtrl(p *Plugin, t time.Duration) *ctrl {
	return &ctrl{
		p:         p,
		timeoutCh: time.After(t),
		linesCh:   make(chan string),
		waitCh:    make(chan error),
	}
}

// fatal 处理致命错误
func (c *ctrl) fatal(err error) {
	c.err = err
	c.open()
	c.kill()
}

// isFatal 检查是否发生了致命错误
func (c *ctrl) isFatal() bool {
	return c.err != nil
}

// close 关闭通信通道
func (c *ctrl) close() {
	c.connCh = nil
	c.objsCh = nil
}

// open 打开通信通道
func (c *ctrl) open() {
	c.connCh = c.p.connCh
	c.objsCh = c.p.objsCh
}

// ready 处理插件就绪消息
func (c *ctrl) ready(val string) bool {
	var err error

	if err := c.parseReady(val); err != nil {
		c.fatal(err)
		return false
	}

	c.client, err = dialAuthRpc(c.secret, c.proto, c.addr, c.p.initTimeout)
	if err != nil {
		c.fatal(err)
		return false
	}

	// 连接后删除临时套接字
	if c.proto == "unix" {
		if err := os.Remove(c.addr); err != nil {
			c.p.handler.Error(errors.New("无法删除临时套接字: " + err.Error()))
		}
	}

	// 解除就绪时的超时
	c.timeoutCh = nil

	return true
}

// readOutput 读取子进程的输出
func (c *ctrl) readOutput(r io.Reader) {
	scanner := bufio.NewScanner(r)

	for scanner.Scan() {
		c.linesCh <- scanner.Text()
	}
}

// waitErr 报告等待错误
func (c *ctrl) waitErr(pidCh chan<- int, err error) {
	close(pidCh)
	c.waitCh <- err
}

// wait 等待子进程
func (c *ctrl) wait(pidCh chan<- int, exe string, params ...string) {
	defer close(c.waitCh)

	cmd := exec.Command(exe, params...)

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		c.waitErr(pidCh, err)
		return
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		c.waitErr(pidCh, err)
		return
	}
	if err := cmd.Start(); err != nil {
		c.waitErr(pidCh, err)
		return
	}

	pidCh <- cmd.Process.Pid
	close(pidCh)

	c.readOutput(stdout)
	c.readOutput(stderr)

	c.waitCh <- cmd.Wait()
}

// kill 终止子进程
func (c *ctrl) kill() {
	if c.proc == nil {
		return
	}
	// 这里忽略错误，因为在进程结束后可能已经调用了Kill
	c.proc.Kill()
	c.proc = nil
}

// parseReady 解析就绪消息
func (c *ctrl) parseReady(str string) error {
	if !strings.HasPrefix(str, "proto=") {
		return errInvalidMessage
	}
	str = str[6:]
	s := strings.IndexByte(str, ' ')
	if s < 0 {
		return errInvalidMessage
	}
	proto := str[0:s]
	if proto != "unix" && proto != "tcp" {
		return errInvalidMessage
	}
	c.proto = proto

	str = str[s+1:]
	if !strings.HasPrefix(str, "addr=") {
		return errInvalidMessage
	}
	c.addr = str[5:]

	return nil
}

// objects 为请求者复制对象列表
func (c *ctrl) objects() []string {
	list := make([]string, len(c.objs)-1)
	for i, j := 0, 0; i < len(c.objs); i++ {
		if c.objs[i] == internalObject {
			continue
		}
		list[j] = c.objs[i]
		j = j + 1
	}
	return list
}

// -----------------------------------------------------------------------------
// 插件运行主循环
// -----------------------------------------------------------------------------

// run 是插件的主事件循环
func (p *Plugin) run() {
	// 设置Unix套接字目录
	if p.unixdir == "" {
		p.unixdir = os.TempDir()
	}

	// 准备命令行参数
	params := []string{
		"-pingo:prefix=" + string(p.meta),
		"-pingo:proto=" + p.proto,
	}
	if p.proto == "unix" && p.unixdir != "" {
		params = append(params, "-pingo:unixdir="+p.unixdir)
	}
	for i := 0; i < len(p.params); i++ {
		params = append(params, p.params[i])
	}

	// 创建控制器并启动子进程
	c := newCtrl(p, p.initTimeout)

	pidCh := make(chan int)
	go c.wait(pidCh, p.exe, params...)
	pid := <-pidCh

	if pid != 0 {
		if proc, err := os.FindProcess(pid); err == nil {
			c.proc = proc
		}
	}

	// 主事件循环
	for {
		select {
		// 初始化超时
		case <-c.timeoutCh:
			c.fatal(errRegistrationTimeout)

		// 处理连接请求
		case r := <-c.connCh:
			if c.isFatal() {
				r.err = c.err
				r.wr.done()
				continue
			}

			r.client = c.client
			r.wr.done()

		// 处理对象列表请求
		case o := <-c.objsCh:
			if c.isFatal() {
				o.err = c.err
				o.wr.done()
				continue
			}

			o.list = c.objects()
			o.wr.done()

		// 处理子进程输出
		case line := <-c.linesCh:
			key, val := p.meta.parse(line)
			switch key {
			case "auth-token":
				c.secret = val
			case "fatal":
				if err := parseError(val); err != nil {
					c.fatal(err)
				} else {
					c.fatal(errors.New(val))
				}
			case "error":
				if err := parseError(val); err != nil {
					p.handler.Print(err)
				} else {
					p.handler.Print(errors.New(val))
				}
			case "objects":
				c.objs = strings.Split(val, ", ")
			case "ready":
				if !c.ready(val) {
					continue
				}
				// 开始接受调用
				c.open()
			default:
				p.handler.Print(line)
			}

		// 处理终止请求
		case wr := <-p.killCh:
			if c.waitCh == nil {
				wr.done()
				continue
			}

			// 如果不接受调用，立即终止
			if c.connCh == nil || c.client == nil {
				c.kill()
			} else {
				// 确保在进程不遵守Exit时将其终止
				go func(pid int, t time.Duration) {
					<-time.After(t)

					if proc, err := os.FindProcess(pid); err == nil {
						proc.Kill()
					}
				}(pid, p.exitTimeout)

				c.client.Call(internalObject+".Exit", 0, nil)
			}

			if c.client != nil {
				c.client.Close()
			}

			// 不接受调用
			c.close()

			// 当子进程退出时，通过"over"发回信号
			c.over = wr

		// 处理子进程退出
		case err := <-c.waitCh:
			if err != nil {
				if _, ok := err.(*exec.ExitError); !ok {
					p.handler.Error(err)
				}
				c.fatal(err)
			}

			// 向通过killCh终止我们的人发送信号，表示我们已完成
			if c.over != nil {
				c.over.done()
			}

			c.proc = nil
			c.waitCh = nil
			c.linesCh = nil

		// 处理退出请求
		case <-p.exitCh:
			return
		}
	}
}
