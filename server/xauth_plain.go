package server

type saslPlainAuth struct {}

func (spa *saslPlainAuth) handleStart(mechanism *string, data []byte, initial_response []byte) *Response {
	return nil
}

func (spa *saslPlainAuth) handleContinue(data []byte) *Response {
	return nil
}
