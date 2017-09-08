// Copyright 2017 PingCAP, Inc.
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

package util

import (
	"github.com/juju/errors"
	"github.com/pingcap/tipb/go-mysqlx/Datatypes"
)

func getValue(any Mysqlx_Datatypes.Any) (interface{}, interface{}, error) {
	switch any.GetType() {
	case Mysqlx_Datatypes.Any_SCALAR:
		datum := any.GetScalar()
		switch datum.GetType() {
		case Mysqlx_Datatypes.Scalar_V_SINT:
			return datum.GetVSignedInt(), nil, nil
		case Mysqlx_Datatypes.Scalar_V_UINT:
			return datum.GetVUnsignedInt(), nil, nil
		case Mysqlx_Datatypes.Scalar_V_NULL:
			return nil, nil, nil
		case Mysqlx_Datatypes.Scalar_V_OCTETS:
			oct := datum.GetVOctets()
			return oct.GetValue(), oct.GetContentType(), nil
		case Mysqlx_Datatypes.Scalar_V_DOUBLE:
			return datum.GetVDouble(), nil, nil
		case Mysqlx_Datatypes.Scalar_V_FLOAT:
			return datum.GetVFloat(), nil, nil
		case Mysqlx_Datatypes.Scalar_V_BOOL:
			return datum.GetVBool(), nil, nil
		case Mysqlx_Datatypes.Scalar_V_STRING:
			str := datum.GetVString()
			return str.GetValue(), str.GetCollation(), nil
		}
	}
	return nil, nil, errors.New("wrong type")
}

// GetSint gets signed int.
func GetSint(any Mysqlx_Datatypes.Any) (int64, error) {
	data, _, err := getValue(any)
	if err != nil {
		return 0, errors.Trace(err)
	}
	return data.(int64), nil
}

// GetUint gets unsigned int.
func GetUint(any Mysqlx_Datatypes.Any) (uint64, error) {
	data, _, err := getValue(any)
	if err != nil {
		return 0, errors.Trace(err)
	}
	return data.(uint64), nil
}

// GetOct gets octets
func GetOct(any Mysqlx_Datatypes.Any) ([]byte, uint32, error) {
	data, tp, err := getValue(any)
	if err != nil {
		return nil, 0, errors.Trace(err)
	}
	return data.([]byte), tp.(uint32), nil
}

// GetDouble gets double.
func GetDouble(any Mysqlx_Datatypes.Any) (float64, error) {
	data, _, err := getValue(any)
	if err != nil {
		return 0, errors.Trace(err)
	}
	return data.(float64), nil
}

// GetFloat gets float.
func GetFloat(any Mysqlx_Datatypes.Any) (float32, error) {
	data, _, err := getValue(any)
	if err != nil {
		return 0, errors.Trace(err)
	}
	return data.(float32), nil
}

// GetBool gets bool.
func GetBool(any Mysqlx_Datatypes.Any) (bool, error) {
	data, _, err := getValue(any)
	if err != nil {
		return false, errors.Trace(err)
	}
	return data.(bool), nil
}

// GetString gets string.
func GetString(any Mysqlx_Datatypes.Any) ([]byte, uint64, error) {
	data, coll, err := getValue(any)
	if err != nil {
		return nil, 0, errors.Trace(err)
	}
	return data.([]byte), coll.(uint64), nil
}
