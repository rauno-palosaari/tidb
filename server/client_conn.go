// Copyright 2015 PingCAP, Inc.
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
	"net"
	"sync/atomic"

	"github.com/ngaut/log"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/xprotocol/xpacketio"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/arena"
)

type clientConn interface {

	handshake() error

	Run()

	isKilled() bool

	Cancel(query bool)

	id() uint32

	showProcess() util.ProcessInfo
}

func createClientConn(conn net.Conn, s *Server) clientConn {
	switch s.typ {
	case MysqlProtocol:
		return &mysqlClientConn{
			conn:         conn,
			pkt:          newPacketIO(conn),
			server:       s,
			connectionID: atomic.AddUint32(&baseConnID, 1),
			collation:    mysql.DefaultCollationID,
			alloc:        arena.NewAllocator(32 * 1024),
			salt:         util.RandomBuf(mysql.ScrambleLength),
		}
	case MysqlXProtocol:
		return &mysqlXClientConn{
			conn:         conn,
			pkt:          xpacketio.NewXPacketIO(conn),
			server:       s,
			capability:   defaultCapability,
			connectionID: atomic.AddUint32(&baseConnID, 1),
			collation:    mysql.DefaultCollationID,
			alloc:        arena.NewAllocator(32 * 1024),
			salt:         util.RandomBuf(mysql.ScrambleLength),
		}
	default:
		log.Error("unknown server type.")
		return nil
	}
}
