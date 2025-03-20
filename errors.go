// Copyright 2015 Giulio Iotti. All rights reserved.
// 使用此源代码受MIT风格许可证约束
// 许可证可在LICENSE文件中找到。

package pingo

import (
	"errors"
	"strings"
)

// -----------------------------------------------------------------------------
// 错误代码常量
// -----------------------------------------------------------------------------

const (
	// 连接失败错误码
	errorCodeConnFailed = "err-connection-failed"
	// HTTP服务错误码
	errorCodeHttpServe = "err-http-serve"
)

// -----------------------------------------------------------------------------
// 错误类型定义
// -----------------------------------------------------------------------------

// ErrConnectionFailed 表示连接到外部插件失败时报告的错误。
// 当插件进程无法启动或网络连接无法建立时可能会出现此错误。
type ErrConnectionFailed error

// ErrHttpServe 表示外部插件无法开始监听调用时报告的错误。
// 当插件无法绑定到请求的地址或端口时可能会出现此错误。
type ErrHttpServe error

// ErrInvalidMessage 表示外部插件打印的消息无效时报告的错误。
// 当收到的消息格式错误或缺少必要信息时可能会出现此错误。
type ErrInvalidMessage error

// ErrRegistrationTimeout 表示插件在注册超时到期前未能注册时报告的错误。
// 当插件进程启动但未能及时完成初始化时可能会出现此错误。
type ErrRegistrationTimeout error

// -----------------------------------------------------------------------------
// 错误处理函数
// -----------------------------------------------------------------------------

// parseError 解析来自插件的错误消息行
// 它从行中提取错误代码和消息，并返回相应类型的错误
// 参数:
//   - line: 包含错误信息的文本行
//
// 返回:
//   - 根据错误代码转换为特定错误类型的错误，或者原始错误
func parseError(line string) error {
	parts := strings.SplitN(line, ": ", 2)
	if parts[0] == "" {
		return nil
	}

	err := errors.New(parts[1])

	switch parts[0] {
	case errorCodeConnFailed:
		return ErrConnectionFailed(err)
	case errorCodeHttpServe:
		return ErrHttpServe(err)
	}

	return err
}
