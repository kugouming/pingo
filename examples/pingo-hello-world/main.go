package main

import (
	"fmt"

	"github.com/kugouming/pingo"
)

type Plugin struct{}

func (p *Plugin) SayHello(name string, msg *string) error {
	*msg = fmt.Sprintf("Hello %s", name)
	return nil
}

func main() {
	plugin := &Plugin{}

	pingo.Register(plugin)
	pingo.Run()
}
