package plugo

import "flag"

// Register a new object this plugin exports. The object must be
// an exported symbol and obey all rules an object in the standard
// "rpc" module has to obey.
//
// Register will panic if called after Run.
func Register(obj interface{}) {
	if defaultServer.running {
		panic("Do not call Register after Run")
	}
	defaultServer.register(obj)
}

// Run will start all the necessary steps to make the plugin available.
func Run() error {
	if !flag.Parsed() {
		flag.Parse()
	}
	return defaultServer.run()
}
