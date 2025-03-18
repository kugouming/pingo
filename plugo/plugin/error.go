package plugin

import (
	"errors"
	"log"
	"plugo/config"
)

var (
	errInvalidMessage      = config.ErrInvalidMessage(errors.New("Invalid ready message"))
	errRegistrationTimeout = config.ErrRegistrationTimeout(errors.New("Registration timed out"))
)

type ErrorHandler interface {
	Error(error)
	Print(interface{})
}

// Default error handler implementation. Uses the default logging facility from the
// Go standard library.
type DefaultErrorHandler struct{}

// Constructor for default error handler.
func NewDefaultErrorHandler() *DefaultErrorHandler {
	return &DefaultErrorHandler{}
}

// Log via default standard library facility prepending the "error: " string.
func (e *DefaultErrorHandler) Error(err error) {
	log.Print("error: ", err)
}

// Log via default standard library facility.
func (e *DefaultErrorHandler) Print(s interface{}) {
	log.Print(s)
}
