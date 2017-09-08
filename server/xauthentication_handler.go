package server

import (
	"github.com/ngaut/log"
)

type Status int32

const (
	Ongoing   Status = iota
	Succeeded
	Failed
	Error
)

type Response struct {
	data    string
	status  Status
	errCode uint16
}

type AuthenticationHandler interface {
	handleStart(mechanism *string, data []byte, initial_response []byte) *Response
	handleContinue(data []byte) *Response
}

func (xa *XAuth)createAuthHandler(method string) AuthenticationHandler {
	switch method {
	case "MYSQL41":
		return &saslMysql41Auth{
			m_state:  S_starting,
			xauth: xa,
		}
	case "PLAIN":
		return &saslPlainAuth{}
	default:
		log.Error("unknown XAuth handler type.")
		return nil
	}
}
