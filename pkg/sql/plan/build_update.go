// Copyright 2021 - 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func buildTableUpdate(stmt *tree.Update, ctx CompilerContext) (p *Plan, err error) {
	tblInfo, err := getUpdateTableInfo(ctx, stmt)
	if err != nil {
		return nil, err
	}
	// new logic
	builder := NewQueryBuilder(plan.Query_SELECT, ctx)
	queryBindCtx := NewBindContext(builder, nil)
	lastNodeId, err := selectUpdateTables(builder, queryBindCtx, stmt, tblInfo)
	if err != nil {
		return nil, err
	}

	// build update expr
	updateExprs, err := buildUpdateExpr(builder, tblInfo)
	if err != nil {
		return nil, err
	}

	sourceStep := builder.appendStep(lastNodeId)
	query, err := builder.createQuery()
	if err != nil {
		return nil, err
	}
	builder.qry.Steps = append(builder.qry.Steps[:sourceStep], builder.qry.Steps[sourceStep+1:]...)

	// append sink node
	lastNodeId = appendSinkNode(builder, queryBindCtx, lastNodeId)
	sourceStep = builder.appendStep(lastNodeId)

	beginIdx := 0
	for i, tableDef := range tblInfo.tableDefs {
		updateBindCtx := NewBindContext(builder, nil)
		thisIdx := beginIdx
		beginIdx = beginIdx + len(tableDef.Cols)
		err = buildUpdatePlans(ctx, builder, updateBindCtx, tblInfo.objRef[i], tableDef, updateExprs[i], thisIdx, sourceStep)
		if err != nil {
			return nil, err
		}
	}
	if err != nil {
		return nil, err
	}

	query.StmtType = plan.Query_UPDATE
	return &Plan{
		Plan: &plan.Plan_Query{
			Query: query,
		},
	}, err
}

func isSameColumnType(t1 *Type, t2 *Type) bool {
	if t1.Id != t2.Id {
		return false
	}
	if t1.Width == t2.Width && t1.Scale == t2.Scale {
		return true
	}
	return true
}

func buildUpdateExpr(builder *QueryBuilder, info *dmlTableInfo) ([]map[int]*Expr, error) {
	updateExprs := make([]map[int]*Expr, len(info.tableDefs))
	var err error
	for idx, tableDef := range info.tableDefs {
		binder := NewUpdateBinder(builder.GetContext(), nil, nil, tableDef.Cols)
		updateCols := info.updateKeys[idx]
		newExprs := make(map[int]*Expr)
		var newExpr *Expr
		for i, col := range tableDef.Cols {
			if oldExpr, exists := updateCols[col.Name]; exists {
				if _, ok := oldExpr.(*tree.DefaultVal); ok {
					newExpr, err = getDefaultExpr(builder.GetContext(), col)
					if err != nil {
						return nil, err
					}
				} else {
					newExpr, err = binder.BindExpr(oldExpr, 0, true)
					if err != nil {
						return nil, err
					}
				}
				newExpr, err = forceCastExpr(builder.GetContext(), newExpr, col.Typ)
				if err != nil {
					return nil, err
				}
				newExprs[i] = newExpr
			}
		}
		updateExprs[idx] = newExprs
	}
	return updateExprs, nil
}

func selectUpdateTables(builder *QueryBuilder, bindCtx *BindContext, stmt *tree.Update, tableInfo *dmlTableInfo) (int32, error) {
	fromTables := &tree.From{
		Tables: stmt.Tables,
	}
	var selectList []tree.SelectExpr

	// append  table.* to project list
	columnsSize := 0
	var aliasList = make([]string, len(tableInfo.alias))
	for alias, i := range tableInfo.alias {
		aliasList[i] = alias
	}
	for i, alias := range aliasList {
		for _, col := range tableInfo.tableDefs[i].Cols {
			e, _ := tree.NewUnresolvedName(builder.GetContext(), alias, col.Name)
			columnsSize = columnsSize + 1
			selectList = append(selectList, tree.SelectExpr{
				Expr: e,
			})
		}
	}

	selectAst := &tree.Select{
		Select: &tree.SelectClause{
			Distinct: false,
			Exprs:    selectList,
			From:     fromTables,
			Where:    stmt.Where,
		},
		OrderBy: stmt.OrderBy,
		Limit:   stmt.Limit,
		With:    stmt.With,
	}
	//ftCtx := tree.NewFmtCtx(dialect.MYSQL)
	//selectAst.Format(ftCtx)
	//sql := ftCtx.String()
	//fmt.Print(sql)
	return builder.buildSelect(selectAst, bindCtx, false)
}
