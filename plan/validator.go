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

package plan

import (
	"fmt"
	"math"
	"strings"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/util/charset"
	"github.com/pingcap/tidb/util/dashbase"
	"github.com/pingcap/tidb/util/types"
)

// Validate checkes whether the node is valid.
func Validate(node ast.Node, inPrepare bool) error {
	v := validator{inPrepare: inPrepare}
	node.Accept(&v)
	return v.err
}

// validator is an ast.Visitor that validates
// ast Nodes parsed from parser.
type validator struct {
	err           error
	wildCardCount int
	inPrepare     bool
	inAggregate   bool
}

func (v *validator) Enter(in ast.Node) (out ast.Node, skipChildren bool) {
	switch node := in.(type) {
	case *ast.AggregateFuncExpr:
		if v.inAggregate {
			// Aggregate function can not contain aggregate function.
			v.err = ErrInvalidGroupFuncUse
			return in, true
		}
		v.inAggregate = true
	case *ast.CreateTableStmt:
		v.checkCreateTableGrammar(node)
		if v.err != nil {
			return in, true
		}
	case *ast.CreateIndexStmt:
		v.checkCreateIndexGrammar(node)
		if v.err != nil {
			return in, true
		}
	case *ast.AlterTableStmt:
		v.checkAlterTableGrammar(node)
		if v.err != nil {
			return in, true
		}
	}
	return in, false
}

func (v *validator) Leave(in ast.Node) (out ast.Node, ok bool) {
	switch x := in.(type) {
	case *ast.AggregateFuncExpr:
		v.inAggregate = false
	case *ast.CreateTableStmt:
		v.checkDashbase(x)
		v.checkAutoIncrement(x)
	case *ast.ParamMarkerExpr:
		if !v.inPrepare {
			v.err = parser.ErrSyntax.Gen("syntax error, unexpected '?'")
			return
		}
	case *ast.Limit:
		if x.Count == nil {
			break
		}
		if _, isParamMarker := x.Count.(*ast.ParamMarkerExpr); isParamMarker {
			break
		}
		// We only accept ? and uint64 for count/offset in parser.y
		var count, offset uint64
		if x.Count != nil {
			count, _ = x.Count.GetValue().(uint64)
		}
		if x.Offset != nil {
			offset, _ = x.Offset.GetValue().(uint64)
		}
		if count > math.MaxUint64-offset {
			x.Count.SetValue(math.MaxUint64 - offset)
		}
	}

	return in, v.err == nil
}

func checkAutoIncrementOp(colDef *ast.ColumnDef, num int) (bool, error) {
	var hasAutoIncrement bool

	if colDef.Options[num].Tp == ast.ColumnOptionAutoIncrement {
		hasAutoIncrement = true
		if len(colDef.Options) == num+1 {
			return hasAutoIncrement, nil
		}
		for _, op := range colDef.Options[num+1:] {
			if op.Tp == ast.ColumnOptionDefaultValue && !op.Expr.GetDatum().IsNull() {
				return hasAutoIncrement, errors.Errorf("Invalid default value for '%s'", colDef.Name.Name.O)
			}
		}
	}
	if colDef.Options[num].Tp == ast.ColumnOptionDefaultValue && len(colDef.Options) != num+1 {
		if colDef.Options[num].Expr.GetDatum().IsNull() {
			return hasAutoIncrement, nil
		}
		for _, op := range colDef.Options[num+1:] {
			if op.Tp == ast.ColumnOptionAutoIncrement {
				return hasAutoIncrement, errors.Errorf("Invalid default value for '%s'", colDef.Name.Name.O)
			}
		}
	}

	return hasAutoIncrement, nil
}

func isConstraintKeyTp(constraints []*ast.Constraint, colDef *ast.ColumnDef) bool {
	for _, c := range constraints {
		if len(c.Keys) < 1 {
		}
		// If the constraint as follows: primary key(c1, c2)
		// we only support c1 column can be auto_increment.
		if colDef.Name.Name.L != c.Keys[0].Column.Name.L {
			continue
		}
		switch c.Tp {
		case ast.ConstraintPrimaryKey, ast.ConstraintKey, ast.ConstraintIndex,
			ast.ConstraintUniq, ast.ConstraintUniqIndex, ast.ConstraintUniqKey:
			return true
		}
	}

	return false
}

func getStmtTableOption(stmt *ast.CreateTableStmt, tp ast.TableOptionType) *ast.TableOption {
	for _, opt := range stmt.Options {
		if opt.Tp == tp {
			return opt
		}
	}
	return nil
}

func isEngineDashbase(stmt *ast.CreateTableStmt) bool {
	opt := getStmtTableOption(stmt, ast.TableOptionEngine)
	if opt == nil {
		return false
	}
	return strings.EqualFold(opt.StrValue, "Dashbase")
}

func (v *validator) checkDashbase(stmt *ast.CreateTableStmt) {
	if !isEngineDashbase(stmt) {
		return
	}

	// DASHBASE_CONN is required.
	opt := getStmtTableOption(stmt, ast.TableOptionDashbaseConnection)
	if opt == nil {
		v.err = errors.New("Incorrect table definition; DASHBASE_CONN option is required for Dashbase engine tables")
		return
	}
	_, success := dashbase.ParseConnectionOption(opt.StrValue)
	if !success {
		v.err = errors.New("Incorrect table definition; DASHBASE_CONN is not valid")
		return
	}

	primaryKeys := 0

	// PK must be datetime type.
	for _, colDef := range stmt.Cols {
		for _, op := range colDef.Options {
			switch op.Tp {
			case ast.ColumnOptionPrimaryKey:
				primaryKeys++
				if colDef.Tp.Tp != mysql.TypeDatetime {
					v.err = errors.New("Incorrect table definition; Dashbase table primary key column must be datetime type")
					return
				}
			}
		}
	}

	for _, constraint := range stmt.Constraints {
		switch tp := constraint.Tp; tp {
		case ast.ConstraintPrimaryKey:
			// PK must be datetime type.
			primaryKeys++
			if len(constraint.Keys) != 1 {
				v.err = errors.New("Incorrect table definition; Dashbase table primary key must contain only one column")
				return
			}
			for _, colDef := range stmt.Cols {
				if colDef.Name.Name.L == constraint.Keys[0].Column.Name.L {
					if colDef.Tp.Tp != mysql.TypeDatetime {
						v.err = errors.New("Incorrect table definition; Dashbase table primary key column must be datetime type")
						return
					}
					break
				}
			}
		case ast.ConstraintUniq, ast.ConstraintUniqKey, ast.ConstraintUniqIndex:
			// Must not have unique index.
			v.err = fmt.Errorf("Incorrect table definition; Constraint %d not supported in Dashbase table", tp)
			return
		case ast.ConstraintKey, ast.ConstraintIndex:
			// Index must be text type.
			if len(constraint.Keys) != 1 {
				v.err = errors.New("Incorrect table definition; Dashbase table index must contain only one column")
				return
			}
			for _, colDef := range stmt.Cols {
				if colDef.Name.Name.L == constraint.Keys[0].Column.Name.L {
					if colDef.Tp.Tp != mysql.TypeBlob {
						v.err = errors.New("Incorrect table definition; Dashbase table index column must be text type")
						return
					}
					break
				}
			}
		}
	}

	// PK is required.
	if primaryKeys == 0 {
		v.err = errors.New("Incorrect table definition; Dashbase table should have a primary key")
		return
	}
}

func (v *validator) checkAutoIncrement(stmt *ast.CreateTableStmt) {
	var (
		isKey            bool
		count            int
		autoIncrementCol *ast.ColumnDef
	)

	for _, colDef := range stmt.Cols {
		var hasAutoIncrement bool
		for i, op := range colDef.Options {
			ok, err := checkAutoIncrementOp(colDef, i)
			if err != nil {
				v.err = err
				return
			}
			if ok {
				hasAutoIncrement = true
			}
			switch op.Tp {
			case ast.ColumnOptionPrimaryKey, ast.ColumnOptionUniqKey:
				isKey = true
			}
		}
		if hasAutoIncrement {
			count++
			autoIncrementCol = colDef
		}
	}

	if count < 1 {
		return
	}
	if !isKey {
		isKey = isConstraintKeyTp(stmt.Constraints, autoIncrementCol)
	}
	autoIncrementMustBeKey := true
	for _, opt := range stmt.Options {
		if opt.Tp == ast.TableOptionEngine && strings.EqualFold(opt.StrValue, "MyISAM") {
			autoIncrementMustBeKey = false
		}
	}
	if (autoIncrementMustBeKey && !isKey) || count > 1 {
		v.err = errors.New("Incorrect table definition; there can be only one auto column and it must be defined as a key")
	}

	switch autoIncrementCol.Tp.Tp {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeLong,
		mysql.TypeFloat, mysql.TypeDouble, mysql.TypeLonglong, mysql.TypeInt24:
	default:
		v.err = errors.Errorf("Incorrect column specifier for column '%s'", autoIncrementCol.Name.Name.O)
	}
}

func (v *validator) checkCreateTableGrammar(stmt *ast.CreateTableStmt) {
	if stmt.Table == nil || stmt.Table.Name.String() == "" {
		v.err = ddl.ErrWrongTableName.GenByArgs("")
		return
	}

	countPrimaryKey := 0
	for _, colDef := range stmt.Cols {
		if err := checkFieldLengthLimitation(colDef); err != nil {
			v.err = errors.Trace(err)
			return
		}
		countPrimaryKey += isPrimary(colDef.Options)
		if countPrimaryKey > 1 {
			v.err = infoschema.ErrMultiplePriKey
			return
		}
	}
	for _, constraint := range stmt.Constraints {
		switch tp := constraint.Tp; tp {
		case ast.ConstraintKey, ast.ConstraintIndex, ast.ConstraintUniq, ast.ConstraintUniqKey, ast.ConstraintUniqIndex:
			err := checkDuplicateColumnName(constraint.Keys)
			if err != nil {
				v.err = err
				return
			}
		case ast.ConstraintPrimaryKey:
			if countPrimaryKey > 0 {
				v.err = infoschema.ErrMultiplePriKey
				return
			}
			countPrimaryKey++
			err := checkDuplicateColumnName(constraint.Keys)
			if err != nil {
				v.err = err
				return
			}
		}
	}
}

func isPrimary(ops []*ast.ColumnOption) int {
	for _, op := range ops {
		if op.Tp == ast.ColumnOptionPrimaryKey {
			return 1
		}
	}
	return 0
}

func (v *validator) checkCreateIndexGrammar(stmt *ast.CreateIndexStmt) {
	v.err = checkDuplicateColumnName(stmt.IndexColNames)
	return
}

func (v *validator) checkAlterTableGrammar(stmt *ast.AlterTableStmt) {
	specs := stmt.Specs
	for _, spec := range specs {
		if spec.NewColumn != nil {
			if err := checkFieldLengthLimitation(spec.NewColumn); err != nil {
				v.err = err
				return
			}
		}
		switch spec.Tp {
		case ast.AlterTableAddConstraint:
			switch spec.Constraint.Tp {
			case ast.ConstraintKey, ast.ConstraintIndex, ast.ConstraintUniq, ast.ConstraintUniqIndex,
				ast.ConstraintUniqKey:
				v.err = checkDuplicateColumnName(spec.Constraint.Keys)
				if v.err != nil {
					return
				}
			default:
				// Nothing to do now.
			}
		case ast.AlterTableOption:
			for _, opt := range spec.Options {
				if opt.Tp == ast.TableOptionAutoIncrement {
					v.err = ErrAlterAutoID
					return
				}
			}
		default:
			// Nothing to do now.
		}
	}
}

// checkDuplicateColumnName checks if index exists duplicated columns.
func checkDuplicateColumnName(indexColNames []*ast.IndexColName) error {
	for i := 0; i < len(indexColNames); i++ {
		name1 := indexColNames[i].Column.Name
		for j := i + 1; j < len(indexColNames); j++ {
			name2 := indexColNames[j].Column.Name
			if name1.L == name2.L {
				return infoschema.ErrColumnExists.GenByArgs(name2)
			}
		}
	}
	return nil
}

// checkFieldLengthLimitation checks the maximum length of the column.
// See https://dev.mysql.com/doc/refman/5.7/en/storage-requirements.html
func checkFieldLengthLimitation(colDef *ast.ColumnDef) error {
	tp := colDef.Tp
	if tp == nil {
		return nil
	}
	if tp.Flen > math.MaxUint32 {
		return types.ErrTooBigDisplayWidth.Gen("Display width out of range for column '%s' (max = %d)", colDef.Name.Name.O, math.MaxUint32)
	}
	switch tp.Tp {
	case mysql.TypeString:
		if tp.Flen != types.UnspecifiedLength && tp.Flen > mysql.MaxFieldCharLength {
			return types.ErrTooBigFieldLength.Gen("Column length too big for column '%s' (max = %d); use BLOB or TEXT instead", colDef.Name.Name.O, mysql.MaxFieldCharLength)
		}
	case mysql.TypeVarchar:
		maxFlen := mysql.MaxFieldVarCharLength
		cs := tp.Charset
		// TODO: TableDefaultCharset-->DatabaseDefaultCharset-->SystemDefaultCharset.
		// TODO: Change TableOption parser to parse collate.
		// Reference https://github.com/pingcap/tidb/blob/b091e828cfa1d506b014345fb8337e424a4ab905/ddl/ddl_api.go#L185-L204
		if len(tp.Charset) == 0 {
			cs = mysql.DefaultCharset
		}
		desc, err := charset.GetCharsetDesc(cs)
		if err != nil {
			return errors.Trace(err)
		}
		maxFlen /= desc.Maxlen
		if tp.Flen != types.UnspecifiedLength && tp.Flen > maxFlen {
			return types.ErrTooBigFieldLength.Gen("Column length too big for column '%s' (max = %d); use BLOB or TEXT instead", colDef.Name.Name.O, maxFlen)
		}
	case mysql.TypeDouble:
		if tp.Flen != types.UnspecifiedLength && tp.Flen > mysql.PrecisionForDouble {
			return types.ErrWrongFieldSpec.Gen("Incorrect column specifier for column '%s'", colDef.Name.Name.O)
		}
	case mysql.TypeSet:
		if len(tp.Elems) > mysql.MaxTypeSetMembers {
			return types.ErrTooBigSet.Gen("Too many strings for column %s and SET", colDef.Name.Name.O)
		}
	default:
		// TODO: Add more types.
	}
	return nil
}
