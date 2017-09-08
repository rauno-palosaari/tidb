package server

import (
	. "github.com/pingcap/check"
)

type testUtilSuite struct {
}

func (s *testUtilSuite) TestExtractNullTerminatedElement(c *C) {
	xauth41 := &saslMysql41Auth{}
	authZid, authCid, passwd := xauth41.extractNullTerminatedElement([]byte("mysql\0root\0*C6382C4"))
	c.Assert(string(authZid), Equals, "mysql")
	c.Assert(string(authCid), Equals, "root")
	c.Assert(string(passwd), Equals, "*C6382C4")
}
