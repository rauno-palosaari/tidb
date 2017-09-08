package util

import (
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tipb/go-mysqlx"
)

const (
	ErXBadMessage                uint16 = 5000
	ErXCapabilitiesPrepareFailed        = 5001
	ErXCapabilityNotFound               = 5002
	ErXInvalidProtocolData              = 5003

	ErXServiceError     = 5010
	ErXSession          = 5011
	ErXInvalidArgument  = 5012
	ErXMissingArgument  = 5013
	ErXBadInsertData    = 5014
	ErXCmdNumArguments  = 5015
	ErXCmdArgumentType  = 5016
	ErXCmdArgumentValue = 5017

	ErXBadUpdateData     = 5050
	ErXBadTypeOfUpdate   = 5051
	ErXBadColumnToUpdate = 5052
	ErXBadMemberToUpdate = 5053

	ErXBadStatementId          = 5110
	ErXBadCursorId             = 5111
	ErXBadSchema               = 5112
	ErXBadTable                = 5113
	ErXBadProjection           = 5114
	ErXDocIdMissing            = 5115
	ErXDocIdDuplicate          = 5116
	ErXDocRequiredFieldMissing = 5117

	ErXProjBadKeyName = 5120
	ErXBadDocPath     = 5121
	ErXCursorExists   = 5122

	ErXExprBadOperator  = 5150
	ErXExprBadNumArgs   = 5151
	ErXExprMissingArg   = 5152
	ErXExprBadTypeValue = 5153
	ErXExprBadValue     = 5154

	ErXInvalidCollection   = 5156
	ErXInvalidAdminCommand = 5157
	ErXExpectNotOpen       = 5158
	ErXExpectFailed        = 5159

	ErXExpectBadCondition              = 5160
	ErXExpectBadConditionValue         = 5161
	ErXInvalidNamespace                = 5162
	ErXBadNotice                       = 5163
	ErXCannotDisableNotice             = 5164
	ErXBadConfiguration                = 5165
	ErXMysqlxAccountMissingPermissions = 5167
)

var (
	ErrXBadMessage = ErrorMessage(ErXBadMessage, "Invalid message", mysql.DefaultMySQLState)
	ErrXNoSuchUser = ErrorMessage(mysql.ErrNoSuchUser, "Invalid user or password", mysql.DefaultMySQLState)
	ErrXAccessDenied = ErrorMessage(mysql.ErrAccessDenied, "Invalid user or password", mysql.DefaultMySQLState)
)

// ErrorMessage returns Mysqlx Error.
func ErrorMessage(errcode uint16, msg string, state string) *Mysqlx.Error {
	code := uint32(errcode)
	sqlState := state
	errMsg := Mysqlx.Error {
		Severity: Mysqlx.Error_ERROR.Enum(),
		Code: &code,
		SqlState: &sqlState,
		Msg: &msg,
	}
	return &errMsg
}
