// Copyright 2015 Giulio Iotti. All rights reserved.
// 使用此源代码受MIT风格许可证约束
// 许可证可在LICENSE文件中找到。

// Package pingo 实现了创建和运行子进程作为插件的基础功能。
package pingo

import (
	"fmt"
	"math/rand"
	"strings"
)

// -----------------------------------------------------------------------------
// 元数据处理
// -----------------------------------------------------------------------------

// msgChannel 是一个字符串类型，用于处理插件消息的前缀
// 它被用作消息的命名空间，确保消息可以被正确识别和解析
type msgChannel string

// string 方法将 msgChannel 类型的值转换为字符串表示形式。
//
// 参数：
// - m: MessageChannel 类型的值
//
// 返回值：
// - string: m 的字符串表示形式
func (m msgChannel) string() string {
	return string(m)
}

// output 输出带有元数据前缀的格式化消息
// 参数:
//   - key: 消息的类型或键
//   - val: 消息的内容或值
//
// 输出格式为: "<元数据前缀>: <key>: <val>"
func (m msgChannel) output(key, val string) {
	fmt.Printf("%s: %s: %s\n", m.string(), key, val)
}

// parse 从一个字符串行中解析元数据消息
// 它检查行是否以正确的元数据前缀开始，然后提取键和值部分
// 参数:
//   - line: 要解析的行文本
//
// 返回:
//   - key: 消息的键部分
//   - val: 消息的值部分
//
// 如果行不是有效的元数据消息，则返回空字符串
func (m msgChannel) parse(line string) (key, val string) {
	// 处理空行
	if line == "" {
		return
	}

	// 确保行长度足够包含元数据前缀
	if len(line) < len(m.string()) {
		return
	}

	// 检查行是否以正确的元数据前缀开始
	if line[0:len(m.string())] != m.string() {
		return
	}

	// 移除前缀和分隔符（": "）
	line = line[len(m.string())+2:]
	// 寻找键和值之间的分隔符
	end := strings.IndexByte(line, ':')
	if end < 0 {
		return
	}

	// 返回解析出的键和值（移除值前面的空格）
	return line[0:end], line[end+2:]
}

// -----------------------------------------------------------------------------
// 随机字符串生成
// -----------------------------------------------------------------------------

// _letters 包含用于生成随机字符串的字符集
var _letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-")

// randstr 生成指定长度的随机字符串
// 此函数用于创建唯一标识符，比如用于套接字名称或认证令牌
// 参数:
//   - n: 生成的随机字符串的长度
//
// 返回:
//   - 由字母、数字和少量特殊字符组成的随机字符串
func randstr(n int) string {
	b := make([]rune, n)
	l := len(_letters)

	for i := range b {
		b[i] = _letters[rand.Intn(l)]
	}

	return string(b)
}
