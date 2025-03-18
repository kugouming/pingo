package plugin

import (
	"context"
	"errors"
	"fmt"
	"net/rpc"
	"os"
	"plugo/config"
	"plugo/helper"
	"strings"
	"time"
)

type Plugin struct {
	exe         string
	proto       string
	unixdir     string
	params      []string
	initTimeout time.Duration
	exitTimeout time.Duration
	handler     ErrorHandler
	running     bool
	meta        helper.Meta
	objsCh      chan *objects
	connCh      chan *conn
	killCh      chan *waiter
	exitCh      chan struct{}
	recovery    *recovery
}

func NewPlugin(proto, path string, params ...string) *Plugin {
	if proto != "unix" && proto != "tcp" {
		panic("Invalid protocol. Specify 'unix' or 'tcp'.")
	}
	p := &Plugin{
		exe:         path,
		proto:       proto,
		params:      params,
		initTimeout: 2 * time.Second,
		exitTimeout: 2 * time.Second,
		handler:     NewDefaultErrorHandler(),
		meta:        helper.Meta("pingo" + helper.Randstr(5)),
		objsCh:      make(chan *objects),
		connCh:      make(chan *conn),
		killCh:      make(chan *waiter),
		exitCh:      make(chan struct{}),
	}
	p.recovery = newRecovery(p, DefaultRecoveryOptions())
	return p
}

// Set the error (and output) handler implementation.  Use this to set a custom implementation.
// By default, standard logging is used.  See ErrorHandler.
//
// Panics if called after Start.
func (p *Plugin) SetErrorHandler(h ErrorHandler) {
	if p.running {
		panic("Cannot call SetErrorHandler after Start")
	}
	p.handler = h
}

// Set the maximum time a plugin is allowed to start up and to shut down.  Empty timeout (zero)
// is not allowed, default will be used.
//
// Default is two seconds.
//
// Panics if called after Start.
func (p *Plugin) SetTimeout(t time.Duration) {
	if p.running {
		panic("Cannot call SetTimeout after Start")
	}
	if t == 0 {
		return
	}
	p.initTimeout = t
	p.exitTimeout = t
}

func (p *Plugin) SetSocketDirectory(dir string) {
	if p.running {
		panic("Cannot call SetSocketDirectory after Start")
	}
	p.unixdir = dir
}

// Default string representation
func (p *Plugin) String() string {
	return fmt.Sprintf("%s %s", p.exe, strings.Join(p.params, " "))
}

// Start will execute the plugin as a subprocess. Start will return immediately. Any first call to the
// plugin will reveal eventual errors occurred at initialization.
//
// Calls subsequent to Start will hang until the plugin has been properly initialized.
func (p *Plugin) Start() {
	p.running = true
	go p.run()
}

// Stop attemps to stop cleanly or kill the running plugin, then will free all resources.
// Stop returns when the plugin as been shut down and related routines have exited.
func (p *Plugin) Stop() {
	wr := newWaiter()
	p.killCh <- wr
	wr.wait()
	p.exitCh <- struct{}{}
}

type conn struct {
	client *rpc.Client
	err    error
	wr     *waiter
}

type objects struct {
	list []string
	err  error
	wr   *waiter
}

func (p *Plugin) Call(name string, args interface{}, resp interface{}) error {
	conn := &conn{wr: newWaiter()}
	p.connCh <- conn
	conn.wr.wait()

	if conn.err != nil {
		return conn.err
	}

	return conn.client.Call(name, args, resp)
}

// Objects returns a list of the exported objects from the plugin. Exported objects used
// internally are not reported.
//
// Like Call, Objects returns any error happened on initialization if called after Start.
func (p *Plugin) Objects() ([]string, error) {
	objects := &objects{wr: newWaiter()}
	p.objsCh <- objects
	objects.wr.wait()

	return objects.list, objects.err
}

func (p *Plugin) run() {
	if p.unixdir == "" {
		p.unixdir = os.TempDir()
	}

	// 使用新的启动器控制器
	c := startPlugin(p)

	for {
		select {
		case <-c.ctx.Done():
			// 上下文超时或取消，检查是否是初始化超时
			if errors.Is(c.ctx.Err(), context.DeadlineExceeded) {
				c.handleError(errRegistrationTimeout)
			}
		case r := <-c.connCh:
			if c.isFatal() {
				r.err = c.err
				r.wr.done()
				continue
			}

			r.client = c.client
			r.wr.done()
		case o := <-c.objsCh:
			if c.isFatal() {
				o.err = c.err
				o.wr.done()
				continue
			}

			o.list = c.objects()
			o.wr.done()
		case line := <-c.linesCh:
			key, val := p.meta.Parse(line)
			switch key {
			case "auth-token":
				c.secret = val
			case "fatal":
				if err := config.ParseError(val); err != nil {
					c.handleError(err)
				} else {
					c.handleError(errors.New(val))
				}
			case "error":
				if err := config.ParseError(val); err != nil {
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
				// Start accepting calls
				c.open()
			default:
				p.handler.Print(line)
			}
		case wr := <-p.killCh:
			if c.waitCh == nil {
				wr.done()
				continue
			}

			// If we don't accept calls, kill immediately
			if c.connCh == nil || c.client == nil {
				// 直接终止进程
				if c.proc != nil {
					c.proc.Kill()
					c.proc = nil
				}
			} else {
				// Be sure to kill the process if it doesn't obey Exit.
				go func(pid int, t time.Duration) {
					<-time.After(t)

					if proc, err := os.FindProcess(pid); err == nil {
						proc.Kill()
					}
				}(c.proc.Pid, p.exitTimeout)

				// 尝试正常退出
				c.client.Call(config.InternalObject+".Exit", 0, nil)
			}

			if c.client != nil {
				c.client.Close()
			}

			// Do not accept calls
			c.close()

			// 取消上下文
			if c.cancel != nil {
				c.cancel()
			}

			// When wait on the subprocess is exited, signal back via "over"
			c.over = wr
		case err := <-c.waitCh:
			if err != nil {
				// 使用统一的错误处理方法
				if p.recovery != nil && !c.isFatal() {
					// 如果成功处理崩溃，则不进行后续处理
					if p.recovery.handleCrash(err) {
						// 重置相关状态，但不触发任何额外信号
						c.proc = nil
						c.waitCh = nil
						c.linesCh = nil
						continue
					}
				}

				c.handleError(err)
			} else if p.recovery != nil {
				// 正常退出，重置重试计数
				p.recovery.resetRetryCount()
			}

			// Signal to whoever killed us (via killCh) that we are done
			if c.over != nil {
				c.over.done()
			}

			c.proc = nil
			c.waitCh = nil
			c.linesCh = nil
		case <-p.exitCh:
			// 确保在退出前取消上下文
			if c.cancel != nil {
				c.cancel()
			}
			return
		}
	}
}
