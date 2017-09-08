package server

import (
	"net"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/mysql"
	xutil "github.com/pingcap/tidb/xprotocol/util"
	"github.com/pingcap/tidb/util/auth"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tipb/go-mysqlx"
	"bytes"
	"github.com/pingcap/tidb/xprotocol"
	"github.com/pingcap/tidb/driver"
)

type authMysql41State int32

const (
	S_starting         authMysql41State = iota
	S_waiting_response
	S_done
	S_error
)

type saslMysql41Auth struct {
	m_state authMysql41State
	m_salt  []byte

	xauth	*XAuth
}

func (spa *saslMysql41Auth) handleStart(mechanism *string, data []byte, initial_response []byte) *Response {
	r := Response{}

	if spa.m_state == S_starting {
		spa.m_salt = util.RandomBuf(mysql.ScrambleLength)
		r.data = string(spa.m_salt)
		r.status = Ongoing
		r.errCode = 0
		spa.m_state = S_waiting_response
	} else {
		r.status = Error
		r.errCode = mysql.ErrNetPacketsOutOfOrder

		spa.m_state = S_error
	}

	return &r
}

func (spa *saslMysql41Auth) handleContinue(data []byte) *Response {
	r := Response{}

	if spa.m_state == S_waiting_response {
		var err *Mysqlx.Error
		var ctx driver.QueryCtx

		dbname, user, passwd := spa.extractNullTerminatedElement(data)
		log.Infof("[YUSP] %s %s %s", string(dbname), string(user), string(passwd))
		xcc := spa.xauth.xcc
		xcc.dbname = string(dbname)
		xcc.user = string(user)
		// Open session and do auth

		ctx, err1 := xcc.server.driver.OpenCtx(uint64(xcc.connectionID), xcc.capability, uint8(xcc.collation), xcc.dbname)
		if err1 != nil {
			err = xutil.ErrXNoSuchUser
		}
		xcc.xsession = xprotocol.CreateXSession(&xcc.alloc, xcc.connectionID, ctx, xcc.pkt, xcc.server.skipAuth())
		xcc.ctx, err1 = xcc.server.driver.OpenCtx(uint64(xcc.connectionID), xcc.capability, uint8(xcc.collation), xcc.dbname)

		if !spa.xauth.xcc.server.skipAuth() {
			// Do Auth
			addr := spa.xauth.xcc.conn.RemoteAddr().String()
			host, _, err1 := net.SplitHostPort(addr)
			if err1 != nil {
				//err = errors.Trace(errAccessDenied.GenByArgs(spa.xauth.User, addr, "YES"))
				err = xutil.ErrXAccessDenied
			}
			if !spa.xauth.xcc.ctx.Auth(&auth.UserIdentity{Username: string(user), Hostname: host},
				passwd, spa.m_salt) {
				err = xutil.ErrXAccessDenied
			}
		}
		if err == nil {
			r.status = Succeeded
			r.errCode = 0
		} else {
			r.status = Failed
			r.data = err.GetMsg()
			r.errCode = uint16(err.GetCode())
		}
		spa.m_state = S_done
	} else {
		spa.m_state = S_error
		r.status = Error
		r.errCode = mysql.ErrNetPacketsOutOfOrder
	}

	return &r
}

func (spa *saslMysql41Auth) extractNullTerminatedElement(data []byte) ([]byte, []byte, []byte) {
	log.Infof("[YUSP] %v", data)
	log.Infof("[YUSP] %s", data)
	slices := bytes.Split(data, []byte{0})
	return slices[0], slices[1], slices[2]
}
