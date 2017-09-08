package server

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/pingcap/tidb/util/types"
	"github.com/pingcap/tidb/driver"
	"github.com/pingcap/tidb/mysql"
)

type testUtilSuite struct {
}

func (s *testUtilSuite) TestDumpTextValue(c *C) {
	defer testleak.AfterTest(c)()

	colInfo := &driver.ColumnInfo{
		Type:    mysql.TypeLonglong,
		Decimal: mysql.NotFixedDec,
	}
	bs, err := dumpTextValue(colInfo, types.NewIntDatum(10))
	c.Assert(err, IsNil)
	c.Assert(string(bs), Equals, "10")

	bs, err = dumpTextValue(colInfo, types.NewUintDatum(11))
	c.Assert(err, IsNil)
	c.Assert(string(bs), Equals, "11")

	colInfo.Type = mysql.TypeFloat
	colInfo.Decimal = 1
	f32 := types.NewFloat32Datum(1.2)
	bs, err = dumpTextValue(colInfo, f32)
	c.Assert(err, IsNil)
	c.Assert(string(bs), Equals, "1.2")

	colInfo.Decimal = 2
	bs, err = dumpTextValue(colInfo, f32)
	c.Assert(err, IsNil)
	c.Assert(string(bs), Equals, "1.20")

	f64 := types.NewFloat64Datum(2.2)
	colInfo.Type = mysql.TypeDouble
	colInfo.Decimal = 1
	bs, err = dumpTextValue(colInfo, f64)
	c.Assert(err, IsNil)
	c.Assert(string(bs), Equals, "2.2")

	colInfo.Decimal = 2
	bs, err = dumpTextValue(colInfo, f64)
	c.Assert(err, IsNil)
	c.Assert(string(bs), Equals, "2.20")

	colInfo.Type = mysql.TypeBlob
	bs, err = dumpTextValue(colInfo, types.NewBytesDatum([]byte("foo")))
	c.Assert(err, IsNil)
	c.Assert(string(bs), Equals, "foo")

	colInfo.Type = mysql.TypeVarchar
	bs, err = dumpTextValue(colInfo, types.NewStringDatum("bar"))
	c.Assert(err, IsNil)
	c.Assert(string(bs), Equals, "bar")

	var d types.Datum

	time, err := types.ParseTime("2017-01-05 23:59:59.575601", mysql.TypeDatetime, 0)
	c.Assert(err, IsNil)
	d.SetMysqlTime(time)
	colInfo.Type = mysql.TypeDatetime
	bs, err = dumpTextValue(colInfo, d)
	c.Assert(err, IsNil)
	c.Assert(string(bs), Equals, "2017-01-06 00:00:00")

	duration, err := types.ParseDuration("11:30:45", 0)
	c.Assert(err, IsNil)
	d.SetMysqlDuration(duration)
	colInfo.Type = mysql.TypeDuration
	bs, err = dumpTextValue(colInfo, d)
	c.Assert(err, IsNil)
	c.Assert(string(bs), Equals, "11:30:45")

	d.SetMysqlDecimal(types.NewDecFromStringForTest("1.23"))
	colInfo.Type = mysql.TypeNewDecimal
	bs, err = dumpTextValue(colInfo, d)
	c.Assert(err, IsNil)
	c.Assert(string(bs), Equals, "1.23")
}
