package helper

import (
	"fmt"
	"math/rand"
	"strings"
)

type Meta string

func (m Meta) Output(key, val string) {
	fmt.Printf("%s: %s: %s\n", string(m), key, val)
}

func (m Meta) Parse(line string) (key, val string) {
	if line == "" {
		return
	}

	if len(line) < len(string(m)) {
		return
	}

	if line[0:len(string(m))] != string(m) {
		return
	}

	line = line[len(string(m))+2:]
	end := strings.IndexByte(line, ':')
	if end < 0 {
		return
	}

	return line[0:end], line[end+2:]
}

var _letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-")

func Randstr(n int) string {
	b := make([]rune, n)
	l := len(_letters)

	for i := range b {
		b[i] = _letters[rand.Intn(l)]
	}

	return string(b)
}
