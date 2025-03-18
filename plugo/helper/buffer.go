package helper

import (
	"bufio"
	"bytes"
	"io"
	"strings"
)

type BufReadWriteCloser struct {
	*bufio.Reader
	r io.ReadWriteCloser
}

func NewBufReadWriteCloser(r io.ReadWriteCloser) *BufReadWriteCloser {
	return &BufReadWriteCloser{Reader: bufio.NewReader(r), r: r}
}

func (b *BufReadWriteCloser) Write(data []byte) (int, error) {
	return b.r.Write(data)
}

func (b *BufReadWriteCloser) Close() error {
	return b.r.Close()
}

func readHeaders(brwc *BufReadWriteCloser) ([]byte, error) {
	var buf bytes.Buffer
	var headerEnd bool

	for {
		b, err := brwc.ReadByte()
		if err != nil {
			return []byte(""), err
		}

		buf.WriteByte(b)

		if b == '\n' {
			if headerEnd {
				break
			}
			headerEnd = true
		} else {
			headerEnd = false
		}
	}

	return buf.Bytes(), nil
}

func ParseHeaders(brwc *BufReadWriteCloser, m map[string]string) error {
	headers, err := readHeaders(brwc)
	if err != nil {
		return err
	}

	r := bytes.NewReader(headers)
	scanner := bufio.NewScanner(r)

	for scanner.Scan() {
		parts := strings.SplitN(scanner.Text(), ": ", 2)
		if parts[0] == "" {
			continue
		}
		m[parts[0]] = parts[1]
	}

	return nil
}
