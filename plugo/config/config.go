package config

import (
	"flag"
	"os"
)

type Config struct {
	Proto   string
	Addr    string
	Prefix  string
	Unixdir string
}

func MakeConfig() *Config {
	c := &Config{}
	flag.StringVar(&c.Proto, "plugo:proto", "unix", "Protocol to use: unix or tcp")
	flag.StringVar(&c.Unixdir, "plugo:unixdir", "", "Alternative directory for unix socket")
	flag.StringVar(&c.Prefix, "plugo:prefix", "plugo", "Prefix to output lines")
	return c
}

// Internal object for plugin control
type PlugoRpc struct{}

// Default constructor for interal object. Do not call manually.
func NewPlugoRpc() *PlugoRpc {
	return &PlugoRpc{}
}

// Internal RPC call to shut down a plugin. Do not call manually.
func (s *PlugoRpc) Exit(status int, unused *int) error {
	os.Exit(status)
	return nil
}
