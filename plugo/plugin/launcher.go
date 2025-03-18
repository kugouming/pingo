package plugin

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net/rpc"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"time"
)

// PluginStatus 表示插件的当前状态
type PluginStatus struct {
	PID          int
	IsRunning    bool
	Uptime       time.Duration
	RestartCount int
	LastError    error
}

// pluginCtrl 负责插件控制和通信
type pluginCtrl struct {
	plugin *Plugin
	objs   []string
	// 协议和地址，用于RPC
	proto, addr string
	// 用于连接到服务器的密钥
	secret string
	// 不可恢复的错误，用作错误发生后的响应
	err error
	// 这是plugin.connCh的别名，允许我们间断性地处理调用
	connCh chan *conn
	// 与上面相同，但用于对象请求
	objsCh chan *objects
	// 上下文和取消函数，用于管理生命周期
	ctx    context.Context
	cancel context.CancelFunc
	// 从子进程Wait获取通知
	waitCh chan error
	// 从子进程获取输出行
	linesCh chan string
	// 响应等待此邮件循环退出的例程
	over *waiter
	// 可执行文件进程
	proc *os.Process
	// 到子进程的RPC客户端
	client *rpc.Client
}

// newPluginCtrl 创建新的插件控制器
// 传入插件对象和上下文，用于超时控制和取消操作
func newPluginCtrl(p *Plugin, ctx context.Context, cancel context.CancelFunc) *pluginCtrl {
	return &pluginCtrl{
		plugin:  p,
		ctx:     ctx,
		cancel:  cancel,
		linesCh: make(chan string),
		waitCh:  make(chan error),
	}
}

// fatal 处理致命错误
func (c *pluginCtrl) fatal(err error) {
	c.err = err
	c.open()

	// 终止进程
	if c.proc != nil {
		c.proc.Kill()
		c.proc = nil
	}

	// 取消上下文
	if c.cancel != nil {
		c.cancel()
	}
}

// handleError 统一处理错误
func (c *pluginCtrl) handleError(err error) {
	if err == nil {
		return
	}

	// 根据错误类型进行处理
	var exitErr *exec.ExitError
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		c.fatal(ErrRegistrationTimeout(err))
	case errors.As(err, &exitErr):
		// 进程非正常退出
		c.plugin.handler.Error(err)
		c.fatal(err)
	default:
		c.fatal(err)
	}
}

// isFatal 检查是否有致命错误
func (c *pluginCtrl) isFatal() bool {
	return c.err != nil
}

// close 关闭通道
func (c *pluginCtrl) close() {
	c.connCh = nil
	c.objsCh = nil
}

// open 打开通道
func (c *pluginCtrl) open() {
	c.connCh = c.plugin.connCh
	c.objsCh = c.plugin.objsCh
}

// ready 处理准备就绪状态
func (c *pluginCtrl) ready(val string) bool {
	var err error

	if err := c.parseReady(val); err != nil {
		c.handleError(err)
		return false
	}

	c.client, err = dialAuthRpc(c.secret, c.proto, c.addr, c.plugin.initTimeout)
	if err != nil {
		c.handleError(err)
		return false
	}

	// 连接成功后删除临时套接字
	if c.proto == "unix" {
		if err := os.Remove(c.addr); err != nil {
			c.plugin.handler.Error(errors.New("Cannot remove temporary socket: " + err.Error()))
		}
	}

	// 解除超时
	if c.cancel != nil {
		c.cancel()
		// 创建新的上下文，不带超时
		c.ctx, c.cancel = context.WithCancel(context.Background())
	}

	return true
}

// waitErr 向等待通道发送错误
func (c *pluginCtrl) waitErr(err error) {
	c.waitCh <- err
}

// setupProcess 设置进程和关联通道
func (c *pluginCtrl) setupProcess(proc *os.Process, outputCh chan string, waitCh chan error) {
	c.proc = proc
	c.linesCh = outputCh
	c.waitCh = waitCh
}

// parseReady 解析就绪消息
func (c *pluginCtrl) parseReady(str string) error {
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
func (c *pluginCtrl) objects() []string {
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

// launcher 负责插件进程的启动和管理
type launcher struct {
	plugin       *Plugin
	cmd          *exec.Cmd
	stdout       io.ReadCloser
	stderr       io.ReadCloser
	pid          int
	isRunning    bool
	startTime    time.Time
	outputLines  chan string
	waitErr      chan error
	ctx          context.Context
	cancel       context.CancelFunc
	restartCount int
	lastError    error
}

// newLauncher 创建新的插件启动器
func newLauncher(p *Plugin, ctx context.Context, cancel context.CancelFunc) *launcher {
	return &launcher{
		plugin:      p,
		isRunning:   false,
		startTime:   time.Time{},
		outputLines: make(chan string),
		waitErr:     make(chan error, 1),
		ctx:         ctx,
		cancel:      cancel,
	}
}

// start 启动插件进程并返回进程ID和通道
func (l *launcher) start(params []string) (*os.Process, chan string, chan error, error) {
	l.cmd = exec.CommandContext(l.ctx, l.plugin.exe, params...)

	var err error
	l.stdout, err = l.cmd.StdoutPipe()
	if err != nil {
		return nil, nil, nil, fmt.Errorf("stdout pipe error: %w", err)
	}

	l.stderr, err = l.cmd.StderrPipe()
	if err != nil {
		// 清理之前分配的资源
		l.stdout.Close()
		return nil, nil, nil, fmt.Errorf("stderr pipe error: %w", err)
	}

	if err := l.cmd.Start(); err != nil {
		// 清理之前分配的资源
		l.stdout.Close()
		l.stderr.Close()
		return nil, nil, nil, fmt.Errorf("process start error: %w", err)
	}

	l.pid = l.cmd.Process.Pid
	l.isRunning = true
	l.startTime = time.Now()

	// 启动读取输出的goroutine
	go l.readOutput(l.stdout)
	go l.readOutput(l.stderr)

	// 监控进程退出
	go func() {
		err := l.cmd.Wait()
		l.isRunning = false
		l.lastError = err
		l.waitErr <- err
	}()

	return l.cmd.Process, l.outputLines, l.waitErr, nil
}

// readOutput 读取插件进程的输出
func (l *launcher) readOutput(r io.Reader) {
	defer func() {
		// 防止读取过程中的panic
		if r := recover(); r != nil {
			l.plugin.handler.Error(fmt.Errorf("panic in readOutput: %v", r))
		}
	}()

	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		select {
		case <-l.ctx.Done():
			return
		case l.outputLines <- scanner.Text():
			// 成功发送
		}
	}

	if err := scanner.Err(); err != nil && !errors.Is(err, io.EOF) {
		l.plugin.handler.Error(fmt.Errorf("error reading output: %w", err))
	}
}

// stop 停止进程并清理资源
func (l *launcher) stop() error {
	if !l.isRunning || l.cmd == nil || l.cmd.Process == nil {
		return nil
	}

	var errs []error

	// 尝试正常终止
	if err := l.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		errs = append(errs, err)
		// 如果正常终止失败，强制终止
		if err := l.cmd.Process.Kill(); err != nil {
			errs = append(errs, err)
		}
	}

	// 等待一小段时间让进程退出
	select {
	case <-l.waitErr:
		// 进程已退出
	case <-time.After(l.plugin.exitTimeout):
		// 超时，强制终止
		if err := l.cmd.Process.Kill(); err != nil {
			errs = append(errs, err)
		}
	}

	// 取消上下文
	if l.cancel != nil {
		l.cancel()
	}

	l.isRunning = false

	if len(errs) > 0 {
		return fmt.Errorf("errors during process termination: %v", errs)
	}

	return nil
}

// status 返回插件状态
func (l *launcher) status() PluginStatus {
	return PluginStatus{
		PID:          l.pid,
		IsRunning:    l.isRunning,
		Uptime:       time.Since(l.startTime),
		RestartCount: l.restartCount,
		LastError:    l.lastError,
	}
}

// buildParams 构建启动参数
func buildLauncherParams(p *Plugin) []string {
	params := []string{
		"-pingo:prefix=" + string(p.meta),
		"-pingo:proto=" + p.proto,
	}

	if p.proto == "unix" && p.unixdir != "" {
		params = append(params, "-pingo:unixdir="+p.unixdir)
	}

	// 添加用户自定义参数
	for i := 0; i < len(p.params); i++ {
		params = append(params, p.params[i])
	}

	return params
}

// startPlugin 启动插件并设置控制器
func startPlugin(p *Plugin) *pluginCtrl {
	// 创建带超时的上下文
	ctx, cancel := context.WithTimeout(context.Background(), p.initTimeout)

	// 创建控制器
	ctrl := newPluginCtrl(p, ctx, cancel)

	// 创建启动器
	l := newLauncher(p, ctx, cancel)

	// 构建启动参数
	params := buildLauncherParams(p)

	// 启动进程并获取通道
	proc, outputCh, waitCh, err := l.start(params)
	if err != nil {
		ctrl.handleError(err)
		cancel() // 确保在错误情况下取消上下文
		return ctrl
	}

	// 设置进程和通道
	ctrl.setupProcess(proc, outputCh, waitCh)

	return ctrl
}
