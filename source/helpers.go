package source

import (
	"database/sql/driver"
	"fmt"
	"golang.org/x/sync/errgroup"
	"runtime"
	"strings"
	"sync"
	u "github.com/araddon/gou"
	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/exec"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/rel"
	"github.com/araddon/qlbridge/schema"
	"github.com/araddon/qlbridge/value"
	"github.com/araddon/qlbridge/vm"
	"github.com/disney/quanta/core"
)

func outputRownumMessages(outCh exec.MessageChan, rs *roaring64.Bitmap, limit, offset int) {

	if rs == nil {
		panic("results bitmap must not be nil.")
	}

	colNames := make(map[string]int, 1)
	colNames["@rownum"] = 0

	batch := make([]uint64, limit)
	outCount := 0
	count := 0
	iter := rs.ManyIterator()
loopExit:
	for {
		n := iter.NextMany(batch)
		if n == 0 {
			break loopExit
		}
		for i := 0; i < n && n > 0; i++ {
			count++
			if count <= offset {
				continue
			}
			vals := make([]driver.Value, 2)
			vals[0] = fmt.Sprintf("%d", batch[i])
			vals[1] = rs
			outCh <- datasource.NewSqlDriverMessageMap(uint64(1), vals, colNames)
			outCount++
			if outCount >= limit {
				break loopExit
			}
		}
	}
}

// Perform functions and inject pseudo-variables into the output
func decorateRow(row []driver.Value, proj *rel.Projection, rowCols map[string]int, columnID uint64) []driver.Value {

	newRow := make([]driver.Value, len(proj.Columns))
	ctx := datasource.NewSqlDriverMessageMap(columnID, row, rowCols)
	for i, v := range proj.Columns {
		ri, rok := rowCols[v.As]
		if rok {
			newRow[i] = fmt.Sprintf("%s", row[ri])
		} else if v.As == "@rownum" {
			newRow[i] = fmt.Sprintf("%d", columnID)
		}
		if v.Col.Expr.NodeType() != "Func" {
			continue
		}
        nodeVal, ok := vm.Eval(ctx, v.Col.Expr)
        if !ok {
            u.Errorf("could not evaluate: %v", v.Col.Expr.String())
			return newRow
        }
		newRow[i] = nodeVal.ToString()
	}
	return newRow
}

func outputProjection(outCh exec.MessageChan, sigChan exec.SigChan, proj *core.Projector,
		colNames, rowCols map[string]int, limit, offset int, isExport, isDistinct bool, pro *rel.Projection) error {

	batchSize := limit
	nThreads := 1
	limitIsBatch := true
	var dupMap sync.Map

	// Parallelize projection for SELECT ... INTO
	if isExport {
		batchSize = 1000
		nThreads = runtime.NumCPU() / 2
		limitIsBatch = false
		proj.Prefetch = true
	}

	var eg errgroup.Group
	for n := 0; n < nThreads; n++ {
		eg.Go(func() error {
			for {
				colIDs, rows, err4 := proj.Next(batchSize)
				if err4 != nil {
					return err4
				}
				if len(rows) == 0 {
					return nil
				}
				for i, columnID := range colIDs {
					rows[i] = decorateRow(rows[i], pro, rowCols, columnID)
					if isDistinct {
						var sb strings.Builder
						for _, fld := range rows[i] {
							sb.WriteString(fld.(string))
						}
						key := sb.String()
						_, loaded := dupMap.LoadOrStore(key, struct{}{})
						if loaded {
							continue
						}
					}
					msg := datasource.NewSqlDriverMessageMap(columnID, rows[i], colNames)
					select {
					case <-sigChan:
						return nil
					case outCh <- msg:
						// continue
					}
				}
				if len(rows) == batchSize && limitIsBatch {
					return nil
				}
			}
		})
	}
	if err := eg.Wait(); err != nil {
		return err
	}
	return nil
}

func createFinalProjectionFromMaps(orig *rel.SqlSelect, aliasMap map[string]*rel.SqlSource, allTables []string, 
		sch *schema.Schema, driverTable string) (*rel.Projection, map[string]int, []string, []string, error) {

	tableMap := make(map[string]*schema.Table)
	projCols := make([]string, 0)
	projColsMap := make(map[string]struct{}, 0)
	joinCols := make([]string, 0)
	for _, i := range allTables {
		table, err := sch.Table(i)
		if err != nil {
			return nil, nil, nil, nil, err
		}
		tableMap[i] = table
		v, ok := aliasMap[i]
		if !ok {
			return nil, nil, nil, nil, fmt.Errorf("no aliasMap entry for %s", i)
		}
		for _, y := range v.JoinNodes() {
			if v.Name == driverTable {
				joinCols = append(joinCols, fmt.Sprintf("%s.%s", v.Name, y.String()))
			}
		}
	}

	ret := rel.NewProjection()
	colNames := make(map[string]int)
	if orig.Star {
		i := 0
		for _, x := range allTables {
			table := tableMap[x]
			for _, y := range table.Fields {
				if y.Collation != "-" && strings.HasPrefix(y.Key, "FK:") {
					continue
				}
				ret.AddColumn(rel.NewColumn(y.Name), y.ValueType())
				colNames[y.Name] = i
				p := fmt.Sprintf("%s.%s", x, y.Name)
				if _, ok := projColsMap[p]; !ok {
					projCols = append(projCols, p)
					projColsMap[p] = struct{}{}
				}
				i++
			}
		}
	} else {
		i := 0
		for _, v := range orig.Columns {
			l, r, isAliased := v.LeftRight()
			_, isFunc := v.Expr.(*expr.FuncNode)
			var table *schema.Table
			if isAliased && !isFunc {
				table = tableMap[aliasMap[l].Name]
			} else {
				table = tableMap[orig.From[0].Name]
			}
			if v.Star || (isAliased && r == "*") {
				for _, y := range table.Fields {
					if y.Collation != "-" && strings.HasPrefix(y.Key, "FK:") {
						continue
					}
					ret.AddColumn(rel.NewColumn(fmt.Sprintf("%s", y.Name)), y.ValueType())
					colNames[y.Name] = i
					p := fmt.Sprintf("%s.%s", table.Name, y.Name)
					if _, ok := projColsMap[p]; !ok {
						projCols = append(projCols, p)
					}
					i++
				}
			} else {
				colName := v.As
				if colName == "@rownum" {
					ret.AddColumn(v, value.IntType)
				} else if vt, ok := table.Column(v.SourceField); ok {
					ret.AddColumn(v, vt)
				} else {
					return nil, nil, nil, nil,
						fmt.Errorf("createFinalProjectionFromMaps: schema lookup fail for %s.%s", table.Name, v.SourceField)
				}
				colNames[colName] = i
				p := fmt.Sprintf("%s.%s", table.Name, v.SourceField)
				if _, ok := projColsMap[p]; !ok {
					projCols = append(projCols, p)
				}
				i++
			}
		}
	}
	ret.Final = true
	return ret, colNames, projCols, joinCols, nil
}

func createFinalProjection(orig *rel.SqlSelect, sch *schema.Schema, driverTable string) (*rel.Projection,
	map[string]int, map[string]int, []string, []string, error) {

	tableMap := make(map[string]*schema.Table)
	aliasMap := make(map[string]*rel.SqlSource)
	table2AliasMap := make(map[string]string)
	projCols := make([]string, 0)
	projColsMap := make(map[string]int, 0)
	joinCols := make([]string, 0)
	isSingleTable := len(orig.From) == 1
	for _, v := range orig.From {
		table, err := sch.Table(v.Name)
		if err != nil {
			return nil, nil, nil, nil, nil, err
		}
		tableMap[v.Name] = table
		alias := v.Alias
		if alias == "" {
			alias = v.Name
		}
		aliasMap[alias] = v
		table2AliasMap[v.Name] = alias
		for _, y := range v.JoinNodes() {
			if v.Name == driverTable {
				joinCols = append(joinCols, fmt.Sprintf("%s.%s", v.Name, y.String()))
			}
		}
	}

	ret := rel.NewProjection()
	colNames := make(map[string]int)
	rownumOffset := 0
	rowCols := make(map[string]int)
	if orig.Star {
		i := 0
		for _, x := range orig.From {
			table := tableMap[x.Name]
			for _, y := range table.Fields {
				if y.Collation != "-" && strings.HasPrefix(y.Key, "FK:") {
					continue
				}
				if isSingleTable {
					ret.AddColumn(rel.NewColumn(y.Name), y.ValueType())
				} else {
					ret.AddColumn(rel.NewColumn(fmt.Sprintf("%s.%s", table2AliasMap[x.Name], y.Name)),
						y.ValueType())
				}
				colNames[y.Name] = i
				p := fmt.Sprintf("%s.%s", x.Name, y.Name)
				if _, ok := projColsMap[p]; !ok {
					rowCols[y.Name] = len(projCols)
					if y.Name != "@rownum" {
						projColsMap[p] = len(projCols) - 1
						projCols = append(projCols, p)
					}
				}
				i++
			}
		}
	} else {
		i := 0
		for _, v := range orig.Columns {
			l, r, isAliased := v.LeftRight()
			var table *schema.Table
			_, isFunc := v.Expr.(*expr.FuncNode)
			if isAliased && !isFunc {
				table = tableMap[aliasMap[l].Name]
			} else {
				table = tableMap[orig.From[0].Name]
			}
			if v.Star || (isAliased && r == "*") {
				for _, y := range table.Fields {
					if y.Collation != "-" && strings.HasPrefix(y.Key, "FK:") {
						continue
					}
					ret.AddColumn(rel.NewColumn(fmt.Sprintf("%s", y.Name)), y.ValueType())
					colNames[y.Name] = i
					p := fmt.Sprintf("%s.%s", table.Name, y.Name)
					if _, ok := projColsMap[p]; !ok {
						rowCols[y.Name] = len(projCols)
						if y.Name != "@rownum" {
							projColsMap[p] = len(projCols) - 1
							projCols = append(projCols, p)
						}
					}
					i++
				}
			} else {
				colName := v.As
				if colName == "@rownum" {
					ret.AddColumn(v, value.IntType)
				} else if isFunc {
					if vt, ok := table.Column(v.SourceField); ok {
						c := rel.NewColumn(colName)
						c.Expr = v.Expr

						c.SourceField = v.SourceField
						ret.AddColumn(c, vt)
					}
				} else if vt, ok := table.Column(v.SourceField); ok {
					ret.AddColumn(v, vt)
				} else {
					return nil, nil, nil, nil, nil,
						fmt.Errorf("createFinalProjection: schema lookup fail for %s.%s", table.Name, v.SourceField)
				}
				colNames[colName] = i + rownumOffset
				if colName == "@rownum" {
					rownumOffset++
					continue
				}
				args := expr.FindAllIdentities(v.Expr)
				for _, arg := range args {
					_, r, _ := arg.LeftRight()
					p := fmt.Sprintf("%s.%s", table.Name, r)
					if _, ok := projColsMap[p]; !ok {
						rowCols[colName] = len(projCols)
						projCols = append(projCols, p)
						projColsMap[p] = len(projCols) - 1
						rowCols[r] = projColsMap[p]
					}
				}
				i++
			}
		}
	}
	ret.Final = true
	return ret, colNames, rowCols, projCols, joinCols, nil
}
