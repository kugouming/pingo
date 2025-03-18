package plugin

import (
	"sync"
	"time"
)

// RecoveryOptions 定义插件恢复策略的选项
type RecoveryOptions struct {
	// 是否启用自动恢复
	Enabled bool
	// 最大重试次数，0表示无限重试
	MaxRetries int
	// 重试间隔，默认为1秒
	RetryInterval time.Duration
	// 指数退避基数，默认为1（不使用指数退避）
	BackoffFactor float64
	// 最大重试间隔
	MaxRetryInterval time.Duration
	// 自定义插件崩溃回调函数
	OnCrash func(p *Plugin, retryCount int, err error)
}

// DefaultRecoveryOptions 返回默认的恢复选项
func DefaultRecoveryOptions() *RecoveryOptions {
	return &RecoveryOptions{
		Enabled:          true,
		MaxRetries:       3,
		RetryInterval:    time.Second,
		BackoffFactor:    2.0,
		MaxRetryInterval: 30 * time.Second,
		OnCrash:          nil,
	}
}

// recovery 是内部结构，管理插件的恢复状态
type recovery struct {
	plugin       *Plugin
	options      *RecoveryOptions
	retryCount   int
	lastExitTime time.Time
	mutex        sync.Mutex
	enabled      bool
}

// newRecovery 创建新的恢复管理器
func newRecovery(p *Plugin, options *RecoveryOptions) *recovery {
	if options == nil {
		options = DefaultRecoveryOptions()
	}

	return &recovery{
		plugin:  p,
		options: options,
		enabled: options.Enabled,
	}
}

// SetRecoveryOptions 设置插件的恢复选项
func (p *Plugin) SetRecoveryOptions(options *RecoveryOptions) {
	if p.running {
		panic("Cannot call SetRecoveryOptions after Start")
	}
	p.recovery = newRecovery(p, options)
}

// 处理插件崩溃
func (r *recovery) handleCrash(err error) bool {
	if !r.enabled {
		return false
	}

	r.mutex.Lock()
	defer r.mutex.Unlock()

	// 更新退出时间
	r.lastExitTime = time.Now()
	r.retryCount++

	// 检查是否超过最大重试次数
	if r.options.MaxRetries > 0 && r.retryCount > r.options.MaxRetries {
		if r.options.OnCrash != nil {
			r.options.OnCrash(r.plugin, r.retryCount, err)
		}
		return false
	}

	// 计算下一次重试间隔（应用指数退避策略）
	interval := r.options.RetryInterval
	for i := 1; i < r.retryCount && r.options.BackoffFactor > 1.0; i++ {
		interval = time.Duration(float64(interval) * r.options.BackoffFactor)
		if interval > r.options.MaxRetryInterval {
			interval = r.options.MaxRetryInterval
			break
		}
	}

	// 触发用户自定义的崩溃回调
	if r.options.OnCrash != nil {
		r.options.OnCrash(r.plugin, r.retryCount, err)
	}

	// 安排重启
	go func() {
		time.Sleep(interval)
		r.restartPlugin()
	}()

	return true
}

// 重启插件
func (r *recovery) restartPlugin() {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	// 标记插件未运行，以便重新启动
	r.plugin.running = false

	// 重启插件
	r.plugin.Start()
}

// 重置重试计数
func (r *recovery) resetRetryCount() {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.retryCount = 0
}
