package sql

import (
	"github.com/pingcap/tidb/xprotocol/xpacketio"
	"github.com/pingcap/tidb/xprotocol/util"
	"github.com/pingcap/tidb/driver"
	"github.com/pingcap/tipb/go-mysqlx/Sql"
	"github.com/pingcap/tipb/go-mysqlx/Resultset"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tipb/go-mysqlx"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/util/arena"
	"github.com/pingcap/tidb/util/types"
)

type XSql struct {
	alloc *arena.Allocator
	ctx driver.QueryCtx
	pkt *xpacketio.XPacketIO
}


func CreateContext(alloc *arena.Allocator, ctx driver.QueryCtx, pkt *xpacketio.XPacketIO) *XSql {
	return &XSql{
		alloc:             alloc,
		ctx:               ctx,
		pkt:               pkt,
	}
}

func (xsql *XSql) DealSQLStmtExecute (msgType Mysqlx.ClientMessages_Type, payload []byte) error {
	var msg Mysqlx_Sql.StmtExecute
	if err := msg.Unmarshal(payload); err != nil {
		return err
	}

	switch msg.GetNamespace() {
	case "xplugin", "mysqlx":
		// TODO: 'xplugin' is deprecated, need to send a notice message.
		xsql.dispatchAdminCmd(msg)
	case "sql", "":
		sql := string(msg.GetStmt())
		log.Infof("[YUSP] %s", sql)
		if err := xsql.executeStmt(sql); err != nil {
			return errors.Trace(err)
		}
	default:
		return errors.New("unknown namespace")
	}
	return nil
}

func (xsql *XSql) executeStmt (sql string) error {
	rs, err := xsql.ctx.Execute(sql)
	if err != nil {
		return err
	}
	for _, r := range rs {
		if err := xsql.writeResultSet(r); err != nil {
			return err
		}
	}
	return xsql.sendExecOk()
}

func (xsql *XSql) sendExecOk() error {
	if err := xsql.pkt.WritePacket(int32(Mysqlx.ServerMessages_SQL_STMT_EXECUTE_OK), nil); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (xsql *XSql) writeResultSet(r driver.ResultSet) error {
	defer r.Close()
	row, err := r.Next()
	if err != nil {
		return errors.Trace(err)
	}
	cols, err := r.Columns()
	if err != nil {
		return errors.Trace(err)
	}

	// Write column information.
	for _, c := range cols {
		tp, err := util.Mysql2XType(c.Type, mysql.HasUnsignedFlag(uint(c.Flag)))
		if err != nil {
			return errors.Trace(err)
		}
		flags := uint32(c.Flag)
		columnMeta := Mysqlx_Resultset.ColumnMetaData {
			Type: tp,
			Name: []byte(c.Name),
			Table: []byte(c.OrgName),
			OriginalTable: []byte(c.OrgTable),
			Schema: []byte(c.Schema),
			Length: &c.ColumnLength,
			Flags: &flags,
		}
		data, err := columnMeta.Marshal()
		if err != nil {
			return errors.Trace(err)
		}
		if err := xsql.pkt.WritePacket(int32(Mysqlx.ServerMessages_RESULTSET_COLUMN_META_DATA), data); err != nil {
			return errors.Trace(err)
		}
	}

	// Write rows.
	for {
		if err != nil {
			return errors.Trace(err)
		}
		if row == nil {
			break
		}
		if err != nil {
			return errors.Trace(err)
		}

		rowData, err := rowToRow(*xsql.alloc, cols, row)
		if err != nil {
			return errors.Trace(err)
		}
		data, err := rowData.Marshal()
		if err != nil {
			return errors.Trace(err)
		}

		if err := xsql.pkt.WritePacket(int32(Mysqlx.ServerMessages_RESULTSET_ROW), data); err != nil {
			return errors.Trace(err)
		}
		row, err = r.Next()
	}

	if err := xsql.pkt.WritePacket(int32(Mysqlx.ServerMessages_RESULTSET_FETCH_DONE), []byte{}); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func rowToRow(alloc arena.Allocator, columns []*driver.ColumnInfo, row []types.Datum) (*Mysqlx_Resultset.Row, error) {
	if len(columns) != len(row) {
		return nil, mysql.ErrMalformPacket
	}
	var fields [][]byte
	for i, val := range row {
		datum, err := driver.DumpDatumToBinary(alloc, columns[i], val)
		if err != nil {
			return nil, errors.Trace(err)
		}
		fields = append(fields, datum)
	}
	return &Mysqlx_Resultset.Row{
		Field: fields,
	}, nil
}
