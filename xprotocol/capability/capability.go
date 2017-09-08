package capability

import (
	"github.com/juju/errors"
	"github.com/pingcap/tipb/go-mysqlx"
	"github.com/pingcap/tipb/go-mysqlx/Connection"
	"github.com/pingcap/tipb/go-mysqlx/Datatypes"
	"github.com/pingcap/tidb/xprotocol/util"
	"github.com/pingcap/tidb/mysql"
)

func getCapability(handler Handler) *Mysqlx_Connection.Capability {
	return handler.Get()
}

// GetCapabilities gets capabilities which to be sent to clint.
func GetCapabilities() *Mysqlx_Connection.Capabilities {
	authHandler := &HandlerAuthMechanisms{
		Values: []string{"MYSQL41"},
	}
	docHandler := &HandlerReadOnlyValue{
		Name: "doc.formats",
		Value: "text",
	}
	nodeHandler := &HandlerReadOnlyValue{
		Name: "node_type",
		Value: "mysql",
	}
	pwdHandler := &HandlerExpiredPasswords{
		Name: "client.pwd_expire_ok",
		Expired: true,
	}
	caps := Mysqlx_Connection.Capabilities{
		Capabilities: []*Mysqlx_Connection.Capability{
			getCapability(authHandler),
			getCapability(docHandler),
			getCapability(nodeHandler),
			getCapability(pwdHandler),
		},
	}
	return &caps
}

// DealInitCapabilitiesSet deals the initial capabilities set message of client.
func DealInitCapabilitiesSet (tp Mysqlx.ClientMessages_Type, msg []byte) error {
	if tp != Mysqlx.ClientMessages_CON_CAPABILITIES_SET {
		return errors.New("bad capabilities set")
	}
	var set Mysqlx_Connection.CapabilitiesSet
	if err := set.Unmarshal(msg); err != nil {
		return errors.Trace(err)
	}
	if set.GetCapabilities() == nil {
		return errors.New("bad capabilities set")
	}
	caps := set.GetCapabilities().GetCapabilities()
	if caps == nil {
		return errors.New("bad capabilities set")
	}
	if caps[0].GetName() != "client.pwd_expire_ok" {
		return errors.New("bad capabilities set")
	}
	if caps[0].GetValue().GetType() != Mysqlx_Datatypes.Any_SCALAR {
		return errors.New("bad capabilities set")
	}
	if caps[0].GetValue().GetScalar().GetType() != Mysqlx_Datatypes.Scalar_V_BOOL {
		return errors.New("bad capabilities set")
	}
	if !caps[0].GetValue().GetScalar().GetVBool() {
		return errors.New("bad capabilities set")
	}
	return nil
}

// DealCapabilitiesGet deals capabilities get message, get message content will always be empty.
func DealCapabilitiesGet (tp Mysqlx.ClientMessages_Type, _ []byte) error {
	if tp != Mysqlx.ClientMessages_CON_CAPABILITIES_GET {
		return errors.New("bad capabilities get")
	}
	return nil
}

// DealSecCapabilitiesSet deals the second capabilities set message.
func DealSecCapabilitiesSet (tp Mysqlx.ClientMessages_Type, msg []byte) error {
	if tp != Mysqlx.ClientMessages_CON_CAPABILITIES_SET {
		return errors.New("bad capabilities set")
	}
	var set Mysqlx_Connection.CapabilitiesSet
	if err := set.Unmarshal(msg); err != nil {
		return errors.Trace(err)
	}
	if set.GetCapabilities() == nil {
		return errors.New("bad capabilities set")
	}
	caps := set.GetCapabilities().GetCapabilities()
	if caps == nil {
		return errors.New("bad capabilities set")
	}
	if caps[0].GetName() != "tls" {
		return errors.New("bad capabilities set")
	}
	if caps[0].GetValue().GetType() != Mysqlx_Datatypes.Any_SCALAR {
		return errors.New("bad capabilities set")
	}
	if caps[0].GetValue().GetScalar().GetType() != Mysqlx_Datatypes.Scalar_V_BOOL {
		return errors.New("bad capabilities set")
	}
	if !caps[0].GetValue().GetScalar().GetVBool() {
		return errors.New("bad capabilities set")
	}
	return nil
}

// CapabilityErrorReport reports capabilities error.
func CapabilityErrorReport() *Mysqlx.Error {
	return util.ErrorMessage(5001, "Capability prepare failed for 'tls'", mysql.DefaultMySQLState)
}
