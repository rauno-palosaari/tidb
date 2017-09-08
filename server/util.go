package server

import (
	"strconv"
	"github.com/pingcap/tidb/util/hack"
	"github.com/pingcap/tidb/driver"
	"github.com/pingcap/tidb/util/types"
	"github.com/pingcap/tidb/mysql"
)

func dumpTextValue(colInfo *driver.ColumnInfo, value types.Datum) ([]byte, error) {
	switch value.Kind() {
	case types.KindInt64:
		return strconv.AppendInt(nil, value.GetInt64(), 10), nil
	case types.KindUint64:
		return strconv.AppendUint(nil, value.GetUint64(), 10), nil
	case types.KindFloat32:
		prec := -1
		if colInfo.Decimal > 0 && int(colInfo.Decimal) != mysql.NotFixedDec {
			prec = int(colInfo.Decimal)
		}
		return strconv.AppendFloat(nil, value.GetFloat64(), 'f', prec, 32), nil
	case types.KindFloat64:
		prec := -1
		if colInfo.Decimal > 0 && int(colInfo.Decimal) != mysql.NotFixedDec {
			prec = int(colInfo.Decimal)
		}
		return strconv.AppendFloat(nil, value.GetFloat64(), 'f', prec, 64), nil
	case types.KindString, types.KindBytes:
		return value.GetBytes(), nil
	case types.KindMysqlTime:
		return hack.Slice(value.GetMysqlTime().String()), nil
	case types.KindMysqlDuration:
		return hack.Slice(value.GetMysqlDuration().String()), nil
	case types.KindMysqlDecimal:
		return hack.Slice(value.GetMysqlDecimal().String()), nil
	case types.KindMysqlEnum:
		return hack.Slice(value.GetMysqlEnum().String()), nil
	case types.KindMysqlSet:
		return hack.Slice(value.GetMysqlSet().String()), nil
	case types.KindMysqlJSON:
		return hack.Slice(value.GetMysqlJSON().String()), nil
	case types.KindMysqlBit:
		return hack.Slice(value.GetMysqlBit().ToString()), nil
	case types.KindMysqlHex:
		return hack.Slice(value.GetMysqlHex().ToString()), nil
	default:
		return nil, errInvalidType.Gen("invalid type %v", value.Kind())
	}
}
