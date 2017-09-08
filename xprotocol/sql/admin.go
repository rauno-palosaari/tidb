package sql

import (
	"github.com/ngaut/log"
	"github.com/juju/errors"
	"github.com/pingcap/tipb/go-mysqlx/Datatypes"
	"github.com/pingcap/tipb/go-mysqlx/Sql"
)

const (
	CountDoc string = "COUNT(CASE WHEN (column_name = 'doc' " +
		"AND data_type = 'json') THEN 1 ELSE NULL END)"
	CountId         = "COUNT(CASE WHEN (column_name = '_id' " +
		"AND generation_expression = " +
		"'json_unquote(json_extract(`doc`,''$._id''))') THEN 1 ELSE NULL END)"
	CountGen        = "COUNT(CASE WHEN (column_name != '_id' " +
		"AND generation_expression RLIKE " +
		"'^(json_unquote[[.(.]])?json_extract[[.(.]]`doc`," +
		"''[[.$.]]([[...]][^[:space:][...]]+)+''[[.).]]{1,2}$') THEN 1 ELSE NULL " +
		"END)"
)

func (xsql *XSql) dispatchAdminCmd (msg Mysqlx_Sql.StmtExecute) error {
	stmt := string(msg.GetStmt())
	args := msg.GetArgs()
	switch stmt {
	case "ping":
	case "list_clients":
	case "kill_client":
	case "create_collection":
	case "drop_collection":
	case "ensure_collection":
	case "create_collection_index":
	case "drop_collection_index":
	case "list_objects":
		if err := xsql.listObjects(args); err != nil {
			return errors.Trace(err)
		}
	case "enable_notices":
	case "disable_notices":
	case "list_notices":
	default:
		return errors.New("unknown statement")
	}
	return nil
}

func (xsql *XSql) ping (args []*Mysqlx_Datatypes.Any) error {
	if len(args) != 0 {
		return errors.New("not enough arguments")
	}
	return xsql.sendExecOk()
}

func (xsql *XSql) listClients () error {
	return nil
}

func (xsql *XSql) killClient () error {
	return nil
}

func (xsql *XSql) createCollection () error {
	return nil
}

func (xsql *XSql) dropCollection () error {
	return nil
}

func (xsql *XSql) ensureCollection () error {
	return nil
}

func (xsql *XSql) createCollectionIndex () error {
	return nil
}

func (xsql *XSql) dropCollectionIndex () error {
	return nil
}

func (xsql *XSql) listObjects (args []*Mysqlx_Datatypes.Any) error {
	if len(args) != 2 {
		return errors.New("not enough arguments")
	}
	if args[0].GetType() != Mysqlx_Datatypes.Any_SCALAR {
		return errors.Errorf("wrong type, need %s, but get %s",
			Mysqlx_Datatypes.Any_Type_name[int32(Mysqlx_Datatypes.Any_SCALAR)],
			Mysqlx_Datatypes.Any_Type_name[int32(args[0].GetType())])
	}
	if args[0].GetScalar().GetType() != Mysqlx_Datatypes.Scalar_V_STRING {
		return errors.Errorf("wrong type, need %s, but get %s",
			Mysqlx_Datatypes.Scalar_Type_name[int32(Mysqlx_Datatypes.Scalar_V_STRING)],
			Mysqlx_Datatypes.Scalar_Type_name[int32(args[0].GetScalar().GetType())])
	}
	if args[1].GetType() != Mysqlx_Datatypes.Any_SCALAR {
		return errors.Errorf("wrong type, need %s, but get %s",
			Mysqlx_Datatypes.Any_Type_name[int32(Mysqlx_Datatypes.Any_SCALAR)],
			Mysqlx_Datatypes.Any_Type_name[int32(args[1].GetType())])
	}
	if args[1].GetScalar().GetType() != Mysqlx_Datatypes.Scalar_V_STRING {
		return errors.Errorf("wrong type, need %s, but get %s",
			Mysqlx_Datatypes.Scalar_Type_name[int32(Mysqlx_Datatypes.Scalar_V_STRING)],
			Mysqlx_Datatypes.Scalar_Type_name[int32(args[1].GetScalar().GetType())])
	}
	schema := string(args[0].GetScalar().GetVString().GetValue())
	pattern := string(args[1].GetScalar().GetVString().GetValue())
	if err := xsql.isSchemaSelectedAndExists(schema); err != nil {
		return errors.Trace(err)
	}
	sql :=
	"SELECT BINARY T.table_name AS name, " +
	"IF(ANY_VALUE(T.table_type) LIKE '%VIEW', " +
	"IF(COUNT(*)=1 AND " +
	CountDoc +
	"=1, 'COLLECTION_VIEW', 'VIEW'), IF(COUNT(*)-2 = " +
	CountGen +
	" AND " +
	CountDoc +
	"=1 AND " +
	CountId +
	"=1, 'COLLECTION', 'TABLE')) AS type " +
	"FROM information_schema.tables AS T " +
	"LEFT JOIN information_schema.columns AS C ON (" +
	"BINARY T.table_schema = C.table_schema AND " +
	"BINARY T.table_name = C.table_name) " +
	"WHERE T.table_schema = "

	if len(schema) == 0 {
		sql += "schema()"
	} else {
		sql += quoteIdentifier(schema)
	}

	if len(pattern) != 0 {
		sql += " AND T.table_name LIKE " + quoteIdentifier(pattern)
	}

	sql += " GROUP BY name ORDER BY name"
	log.Infof("[YUSP] %s", sql)
	if err := xsql.executeStmt(sql); err != nil {
		return errors.Trace(err)
	}
	return xsql.sendExecOk()
}

func (xsql *XSql) enableNotices () error {
	return nil
}

func (xsql *XSql) disableNotices () error {
	return nil
}

func (xsql *XSql) listNotices () error {
	return nil
}

func (xsql *XSql) isSchemaSelectedAndExists (schema string) error {
	sql := "SHOW TABLE"
	if len(schema) != 0 {
		sql = sql + " FROM " + quoteIdentifier(schema)
	}
	if err := xsql.executeStmt(sql); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func quoteIdentifier(str string) string {
	return "`" + str + "`"
}
