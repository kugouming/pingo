package plugo

import (
	"fmt"
	"path"
	"path/filepath"
	"plugo/helper"
)

type connection interface {
	addr() string
	retries() int
}

// tcp is a TCP Server Port number.
type tcp int

// addr returns the address for a TCP connection.
func (t *tcp) addr() string {
	if *t < 1024 {
		// Only use unprivileged ports
		*t = 1023
	}

	*t = *t + 1
	return fmt.Sprintf("127.0.0.1:%d", *t)
}

// retries returns the number of retries for a TCP connection.
func (t *tcp) retries() int {
	return 500
}

// unix is a Unix Domain Socket.
type unix string

// addr returns the address for a Unix Domain Socket.
func (u *unix) addr() string {
	name := helper.Randstr(8)
	if *u != "" {
		name = filepath.FromSlash(path.Join(string(*u), name))
	}
	return name
}

// retries returns the number of retries for a Unix Domain Socket.
func (u *unix) retries() int {
	return 4
}
