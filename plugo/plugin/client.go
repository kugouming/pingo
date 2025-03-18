package plugin

import (
	"io"
	"net/rpc"
)

type client struct {
	*rpc.Client
	secret string
}

func newClient(s string, conn io.ReadWriteCloser) *client {
	return &client{secret: s, Client: rpc.NewClient(conn)}
}

func (c *client) authenticate(w io.Writer) error {
	_, err := io.WriteString(w, "Auth-Token: "+c.secret+"\n\n")
	return err
}
