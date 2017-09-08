package xprotocol

import (
	"github.com/pingcap/tidb/xprotocol/sql"
	"github.com/pingcap/tidb/driver"
	"github.com/pingcap/tidb/xprotocol/xpacketio"
	"github.com/pingcap/tipb/go-mysqlx"
	"github.com/juju/errors"
	"github.com/pingcap/tidb/util/arena"
)

type XSession struct {
	xsql *sql.XSql
}

func CreateXSession(alloc *arena.Allocator, id uint32, ctx driver.QueryCtx, pkt *xpacketio.XPacketIO, skipAuth bool) *XSession {
	return &XSession{
		xsql: sql.CreateContext(alloc, ctx, pkt),
	}
}

func (xs *XSession) HandleMessage(msgType Mysqlx.ClientMessages_Type, payload []byte) error {
	switch msgType {
	case Mysqlx.ClientMessages_SQL_STMT_EXECUTE:
		if err := xs.xsql.DealSQLStmtExecute(msgType, payload); err != nil {
			return err
		}
	default:
		return errors.Errorf("unknown message type %d", msgType)
	}

	return nil
}

