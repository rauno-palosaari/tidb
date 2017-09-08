package notice

import (
	"github.com/pingcap/tipb/go-mysqlx/Notice"
	"github.com/pingcap/tidb/xprotocol/xpacketio"
	"github.com/pingcap/tipb/go-mysqlx"
	"github.com/pingcap/tipb/go-mysqlx/Datatypes"
	"github.com/pingcap/tidb/mysql"
)

type Notice struct {
	noticeType NoticeType
	value      []byte
	pkt        *xpacketio.XPacketIO
}

type NoticeType uint32

const (
	KNoticeWarning                NoticeType = 1
	KNoticeSessionVariableChanged            = 2
	KNoticeSessionStateChanged               = 3
)

func (n *Notice) SendLocalNotice(forceFlush bool) error {
	return n.SendNotice(Mysqlx_Notice.Frame_LOCAL, forceFlush)
}

func (n *Notice) SendNotice(scope Mysqlx_Notice.Frame_Scope, forceFlush bool) error {
	frameType := uint32(n.noticeType)
	msg := Mysqlx_Notice.Frame{
		Type:    &frameType,
		Scope:   &scope,
		Payload: n.value,
	}

	data, err := msg.Marshal()
	if err != nil {
		return err
	}

	n.pkt.WritePacket(int32(Mysqlx.ServerMessages_NOTICE), data)
	return nil
}

func SendOK(pkt *xpacketio.XPacketIO, content *string) error {
	msg := Mysqlx.Ok{
		Msg: content,
	}

	data, err := msg.Marshal()
	if err != nil {
		return err
	}

	notice := Notice{
		noticeType: KNoticeSessionStateChanged,
		value:      data,
		pkt:        pkt,
	}

	return notice.SendLocalNotice(false)
}

func SendClientId(pkt *xpacketio.XPacketIO, sessionId uint32) error {
	param := Mysqlx_Notice.SessionStateChanged_Parameter(Mysqlx_Notice.SessionStateChanged_CLIENT_ID_ASSIGNED)
	scalarType := Mysqlx_Datatypes.Scalar_V_UINT
	id := uint64(sessionId)
	msg := Mysqlx_Notice.SessionStateChanged{
		Param: &param,
		Value: &Mysqlx_Datatypes.Scalar{
			Type:         &scalarType,
			VUnsignedInt: &id,
		},
	}

	data, err := msg.Marshal()
	if err != nil {
		return err
	}

	notice := Notice{
		noticeType: KNoticeSessionStateChanged,
		value:      data,
		pkt:        pkt,
	}

	return notice.SendLocalNotice(false)
}

func SendInitError(pkt *xpacketio.XPacketIO, code *uint16, msg *string) error {
	errCode := uint32(*code)
	sqlState := mysql.DefaultMySQLState
	severity := Mysqlx.Error_Severity(Mysqlx.Error_FATAL)
	mysqlxErr := Mysqlx.Error{
		Code: &errCode,
		SqlState: &sqlState,
		Msg: msg,
		Severity: &severity,
	}

	data, err := mysqlxErr.Marshal()
	if err != nil {
		return err
	}

	return pkt.WritePacket(int32(Mysqlx.ServerMessages_ERROR), data)
}
