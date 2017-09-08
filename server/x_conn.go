// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"io"
	"net"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/util"
	xutil "github.com/pingcap/tidb/xprotocol/util"
	"github.com/pingcap/tidb/xprotocol/xpacketio"
	"github.com/pingcap/tidb/util/arena"
	"github.com/pingcap/tipb/go-mysqlx"
	"github.com/pingcap/tidb/xprotocol/capability"
	"github.com/pingcap/tidb/driver"
	"github.com/pingcap/tidb/xprotocol"
	"github.com/pingcap/tidb/mysql"
)

// mysqlXClientConn represents a connection between server and client,
// it maintains connection specific state, handles client query.
type mysqlXClientConn struct {
	pkt          *xpacketio.XPacketIO // a helper to read and write data in packet format.
	conn         net.Conn
	xauth        XAuth
	xsession     *xprotocol.XSession
	server       *Server           // a reference of server instance.
	capability   uint32            // client capability affects the way server handles client request.
	connectionID uint32            // atomically allocated by a global variable, unique in process scope.
	collation    uint8             // collation used by client, may be different from the collation used by database.
	user         string            // user of the client.
	dbname       string            // default database name.
	salt         []byte            // random bytes used for authentication.
	alloc        arena.Allocator   // an memory allocator for reducing memory allocation.
	lastCmd      string            // latest sql query string, currently used for logging error.
	ctx          driver.QueryCtx   // an interface to execute sql statements.
	attrs        map[string]string // attributes parsed from client handshake response, not used for now.
	killed       bool
}

func (xcc *mysqlXClientConn) Run() {
	defer func() {
		recover()
		xcc.Close()
	}()

	for !xcc.killed {
		tp, payload, err := xcc.pkt.ReadPacket()
		if err != nil {
			if terror.ErrorNotEqual(err, io.EOF) {
				log.Errorf("[%d] read packet error, close this connection %s",
					xcc.connectionID, errors.ErrorStack(err))
			}
			return
		}
		if err = xcc.dispatch(tp, payload); err != nil {
			log.Infof("[XUWT] dispatch msg type(%s), payload(%s)", tp, payload)
			if terror.ErrorEqual(err, terror.ErrResultUndetermined) {
				log.Errorf("[%d] result undetermined error, close this connection %s",
					xcc.connectionID, errors.ErrorStack(err))
				return
			} else if terror.ErrorEqual(err, terror.ErrCritical) {
				log.Errorf("[%d] critical error, stop the server listener %s",
					xcc.connectionID, errors.ErrorStack(err))
				select {
				case xcc.server.stopListenerCh <- struct{}{}:
				default:
				}
				return
			}
			log.Warnf("[%d] dispatch error: %s", xcc.connectionID, err)
			xcc.writeError(err)
		}
	}
}

func (xcc *mysqlXClientConn) Close() error {
	xcc.server.rwlock.Lock()
	delete(xcc.server.clients, xcc.connectionID)
	connections := len(xcc.server.clients)
	xcc.server.rwlock.Unlock()
	connGauge.Set(float64(connections))
	xcc.conn.Close()
	if xcc.ctx != nil {
		return xcc.ctx.Close()
	}
	return nil
}

func (xcc *mysqlXClientConn) handshakeConnection() error {
	log.Infof("[YUSP] begin connection")
	tp, msg, err := xcc.pkt.ReadPacket()
	if err != nil {
		return errors.Trace(err)
	}
	log.Infof("[YUSP] deal first msg")
	if err = capability.DealInitCapabilitiesSet(Mysqlx.ClientMessages_Type(tp), msg); err != nil {
		return errors.Trace(err)
	}
	log.Infof("[YUSP] send first msg")
	if err = xcc.pkt.WritePacket(int32(Mysqlx.ServerMessages_OK), []byte{}); err != nil {
		return errors.Trace(err)
	}
	log.Infof("[YUSP] read sec msg")
	tp, msg, err = xcc.pkt.ReadPacket()
	if err != nil {
		return errors.Trace(err)
	}
	log.Infof("[YUSP] deal sec msg")
	if err = capability.DealCapabilitiesGet(Mysqlx.ClientMessages_Type(tp), msg); err != nil {
		return errors.Trace(err)
	}
	resp, err := capability.GetCapabilities().Marshal()
	if err != nil {
		return errors.Trace(err)
	}
	if err = xcc.pkt.WritePacket(int32(Mysqlx.ServerMessages_CONN_CAPABILITIES), resp); err != nil {
		return errors.Trace(err)
	}
	tp, msg, err = xcc.pkt.ReadPacket()
	if err != nil {
		return errors.Trace(err)
	}
	if err = capability.DealSecCapabilitiesSet(Mysqlx.ClientMessages_Type(tp), msg); err != nil {
		return errors.Trace(err)
	}
	resp, err = capability.CapabilityErrorReport().Marshal()
	if err != nil {
		return errors.Trace(err)
	}
	if err = xcc.pkt.WritePacket(int32(Mysqlx.ServerMessages_ERROR), resp); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (xcc *mysqlXClientConn) handshakeSession() error {
	tp, msg, err := xcc.pkt.ReadPacket()
	if err != nil {
		return errors.Trace(err)
	}

	// Open session and do auth
	var ctx driver.QueryCtx
	ctx, err = xcc.server.driver.OpenCtx(uint64(xcc.connectionID), xcc.capability, uint8(xcc.collation), xcc.dbname)
	if err != nil {
		return errors.Trace(err)
	}
	xcc.xsession = xprotocol.CreateXSession(&xcc.alloc, xcc.connectionID, ctx, xcc.pkt, xcc.server.skipAuth())

	xcc.xauth = *xcc.CreateAuth(xcc.connectionID)
	if err := xcc.xauth.handleMessage(Mysqlx.ClientMessages_Type(tp), msg); err != nil {
		return errors.Trace(err)
	}

	tp, msg, err = xcc.pkt.ReadPacket()
	if err != nil {
		return errors.Trace(err)
	}

	if err := xcc.xauth.handleMessage(Mysqlx.ClientMessages_Type(tp), msg); err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (xcc *mysqlXClientConn) handshake() error {
	if err := xcc.handshakeConnection(); err != nil {
		return err
	}

	if err := xcc.handshakeSession(); err != nil {
		return err
	}

	if xcc.dbname != "" {
		if err := xcc.useDB(xcc.dbname); err != nil {
			return errors.Trace(err)
		}
	}
	xcc.ctx.SetSessionManager(xcc.server)

	return nil
}

func (xcc *mysqlXClientConn) dispatch(tp int32, payload []byte) error {
	msgType := Mysqlx.ClientMessages_Type(tp)
	switch msgType {
	case Mysqlx.ClientMessages_SESS_CLOSE, Mysqlx.ClientMessages_CON_CLOSE, Mysqlx.ClientMessages_SESS_RESET:
		if err := xcc.xauth.HandleReadyMessage(msgType, payload); err != nil {
			return err
		}
	default:
		return xcc.xsession.HandleMessage(msgType, payload)
	}

	return nil
}

func (xcc *mysqlXClientConn) flush() error {
	return xcc.pkt.Flush()
}

func (xcc *mysqlXClientConn) writeError(e error) error {
	var (
		m  *mysql.SQLError
		te *terror.Error
		ok bool
	)
	originErr := errors.Cause(e)
	if te, ok = originErr.(*terror.Error); ok {
		m = te.ToSQLError()
	} else {
		m = mysql.NewErrf(mysql.ErrUnknown, "%s", e.Error())
	}
	errMsg, err := xutil.ErrorMessage(m.Code, m.Message, m.State).Marshal()
	if err != nil {
		return err
	}
	return xcc.pkt.WritePacket(int32(Mysqlx.ServerMessages_ERROR), errMsg)
}

func (xcc *mysqlXClientConn) isKilled() bool {
	return xcc.killed
}

func (xcc *mysqlXClientConn) Cancel(query bool) {
	//xcc.ctx.Cancel()
	if !query {
		xcc.killed = true
	}
}

func (xcc *mysqlXClientConn) id() uint32 {
	return xcc.connectionID
}

func (xcc *mysqlXClientConn) showProcess() util.ProcessInfo {
	//return xcc.ctx.ShowProcess()
	return util.ProcessInfo{}
}

func (xcc *mysqlXClientConn) useDB(db string) (err error) {
	// if input is "use `SELECT`", mysql client just send "SELECT"
	// so we add `` around db.
	_, err = xcc.ctx.Execute("use `" + db + "`")
	if err != nil {
		return errors.Trace(err)
	}
	xcc.dbname = db
	return
}

func (xcc *mysqlXClientConn) CreateAuth(id uint32) *XAuth {
	return &XAuth{
		xcc:               xcc,
		mState:            authenticating,
		mStateBeforeClose: authenticating,
	}
}
