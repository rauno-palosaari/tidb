package server

import (
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tipb/go-mysqlx"
	"github.com/pingcap/tipb/go-mysqlx/Session"
	"github.com/pingcap/tidb/xprotocol/notice"
	"github.com/pingcap/tidb/xprotocol/util"
	"github.com/pingcap/tidb/mysql"
)

type sessionState int32

const (
	// start as Authenticating
	authenticating sessionState = iota
	// once authenticated, we can handle work
	ready
	// connection is closing, but wait for data to flush out first
	closing
)

type XAuth struct {
	xcc *mysqlXClientConn
	authHandler AuthenticationHandler

	mState            sessionState
	mStateBeforeClose sessionState
}

func (xa *XAuth) handleMessage(msgType Mysqlx.ClientMessages_Type, payload []byte) error {
	if xa.mState == authenticating {
		return xa.HandleAuthMessage(msgType, payload)
	} else if xa.mState == ready {
		return xa.HandleReadyMessage(msgType, payload)
	}

	return errors.New("unknown session state")
}

func (xa *XAuth) HandleReadyMessage(msgType Mysqlx.ClientMessages_Type, payload []byte) error {
	switch msgType {
	case Mysqlx.ClientMessages_SESS_CLOSE:
		content := "bye!"
		notice.SendOK(xa.xcc.pkt, &content)
		xa.onClose(false)
		return nil
	case Mysqlx.ClientMessages_CON_CLOSE:
		content := "bye!"
		notice.SendOK(xa.xcc.pkt, &content)
		xa.onClose(false)
		return nil
	case Mysqlx.ClientMessages_SESS_RESET:
		xa.mState = closing
		xa.onSessionReset()
		return nil
	}
	return errors.New("invalid message type")
}

func (xa *XAuth) HandleAuthMessage(msgType Mysqlx.ClientMessages_Type, payload []byte) error {
	var r *Response
	switch msgType {
	case Mysqlx.ClientMessages_SESS_AUTHENTICATE_START:
		var data Mysqlx_Session.AuthenticateStart
		if err := data.Unmarshal(payload); err != nil {
			log.Errorf("Can't Unmarshal message %s, err %s", msgType.String(), err.Error())
			errCode := util.ErXBadMessage
			content := "Invalid message"
			notice.SendInitError(xa.xcc.pkt, &errCode, &content)
			return err
		}

		xa.authHandler = xa.createAuthHandler(*data.MechName)
		if xa.authHandler == nil {
			log.Errorf("Can't create XAuth handler with mech name %s", *data.MechName)
			errCode := uint16(mysql.ErrNotSupportedAuthMode)
			content := "Invalid authentication method " + *data.MechName
			notice.SendInitError(xa.xcc.pkt, &errCode, &content)
			xa.stopAuth()
			return errors.New("invalid authentication method")
		}

		r = xa.authHandler.handleStart(data.MechName, data.AuthData, data.InitialResponse)
	case Mysqlx.ClientMessages_SESS_AUTHENTICATE_CONTINUE:
		var data Mysqlx_Session.AuthenticateContinue
		if err := data.Unmarshal(payload); err != nil {
			errCode := util.ErXBadMessage
			content := "Invalid message"
			notice.SendInitError(xa.xcc.pkt, &errCode, &content)
			return err
		}

		r = xa.authHandler.handleContinue(data.GetAuthData())
	default:
		errCode := util.ErXBadMessage
		content := "Invalid message"
		notice.SendInitError(xa.xcc.pkt, &errCode, &content)
		xa.stopAuth()
		return errors.New("invalid message")
	}

	switch r.status {
	case Succeeded:
		xa.onAuthSuccess(r)
	case Failed:
		xa.onAuthFailure(r)
	default:
		xa.SendAuthContinue(&r.data)
	}

	return nil
}

func (xa *XAuth) onAuthSuccess(r *Response) {
	notice.SendClientId(xa.xcc.pkt, xa.xcc.connectionID)
	xa.stopAuth()
	xa.mState = ready
	xa.SendAuthOk(&r.data)

}

func (xa *XAuth) onAuthFailure(r *Response) {
	errCode := uint16(mysql.ErrAccessDenied)
	notice.SendInitError(xa.xcc.pkt, &errCode, &r.data)
	xa.stopAuth()
}

//@TODO need to implement
func (xa *XAuth) onSessionReset() {
}

func (xa *XAuth) onClose(updateOldState bool) {
	if xa.mState != closing {
		if updateOldState {
			xa.mStateBeforeClose = xa.mState
		}
		xa.mState = closing
	}
}

func (xa *XAuth) stopAuth() {
	xa.authHandler = nil
}

func (xa *XAuth) SendAuthOk(value *string) error {
	msg := Mysqlx_Session.AuthenticateOk{
		AuthData: []byte(*value),
	}

	data, err := msg.Marshal()
	if err != nil {
		return err
	}

	return xa.xcc.pkt.WritePacket(int32(Mysqlx.ServerMessages_SESS_AUTHENTICATE_OK), data)
}

func (xa *XAuth) SendAuthContinue(value *string) error {
	msg := Mysqlx_Session.AuthenticateContinue{
		AuthData: []byte(*value),
	}

	log.Infof("[YUSP] %s", msg.String())
	data, err := msg.Marshal()
	if err != nil {
		return err
	}

	return xa.xcc.pkt.WritePacket(int32(Mysqlx.ServerMessages_SESS_AUTHENTICATE_CONTINUE), data)
}

