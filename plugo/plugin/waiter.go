package plugin

type waiter struct {
	c chan struct{}
}

func newWaiter() *waiter {
	return &waiter{c: make(chan struct{})}
}

func (wr *waiter) wait() {
	<-wr.c
}

func (wr *waiter) done() {
	close(wr.c)
}

func (wr *waiter) reset() {
	wr.c = make(chan struct{})
}
