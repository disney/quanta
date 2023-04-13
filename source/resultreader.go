package source

import (
	"database/sql/driver"
	"fmt"
	"github.com/RoaringBitmap/roaring/roaring64"
	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/exec"
	"github.com/araddon/qlbridge/rel"
	"github.com/disney/quanta/core"
	"github.com/disney/quanta/shared"
	"math"
	"time"
)

var (
	// Ensure we implement TaskRunner
	_ exec.TaskRunner = (*ResultReader)(nil)
)

// ResultReader implements result paging, reading
// - driver.Rows
type ResultReader struct {
	*exec.TaskBase
	finalized bool
	cursor    int
	limit     int
	offset    int
	Vals      [][]driver.Value
	Total     int
	response  *shared.BitmapQueryResponse
	sql       *SQLToQuanta
	conn      *core.Session
}

// ResultReaderNext - A wrapper, allowing us to implement sql/driver Next() interface
// which differs from qlbridge/datasource Next()
type ResultReaderNext struct {
	*ResultReader
}

// NewResultReader - Construct a result reader.
func NewResultReader(conn *core.Session, req *SQLToQuanta, q *shared.BitmapQueryResponse,
	limit, offset int) *ResultReader {
	m := &ResultReader{}
	if req.Ctx == nil {
		u.Errorf("no context? %p", m)
	}
	m.TaskBase = exec.NewTaskBase(req.Ctx)
	m.response = q
	//u.LogTraceDf(u.WARN, 16, "hello")
	//u.Debugf("new resultreader:  sqltopql:%p   sourceplan:%p", req, req.p)
	m.sql = req
	m.limit = limit
	m.offset = offset
	m.conn = conn
	return m
}

// Close the result reader.
func (m *ResultReader) Close() error {
	return nil
}

// Run the result reader as a task.
func (m *ResultReader) Run() error {

	sigChan := m.SigChan()
	outCh := m.MessageOut()
	//defer context.Recover()
	defer func() {
		close(outCh) // closing output channels is the signal to stop
		//m.TaskBase.Close()
		//u.Debugf("nice, finalize ResultReader out: %p  row ct %v", outCh, len(m.Vals))
	}()

	m.finalized = true

	//u.LogTracef(u.WARN, "hello")

	orig := m.sql.Ctx.Stmt.(*rel.SqlSelect)

	sql := m.sql.sel
	if sql == nil {
		u.Warnf("no sql? %p  %#v", m.sql, m.sql)
		return fmt.Errorf("no sql")
	}
	if m.sql.p == nil {
		u.Warnf("no plan????  %#v", m.sql)
		return fmt.Errorf("no plan")
	}
	//cols := m.sql.sel.Columns
	//u.Debugf("m.sql %p %#v", m.sql, m.sql)
	if m.sql == nil {
		return fmt.Errorf("No sqltoquanta?????? ")
	}
	//u.Debugf("%p about to blow up sqltopql: %p", m.sql.p, m.sql)
	if m.sql.p == nil {
		u.Warnf("no plan?")
		return fmt.Errorf("no plan? %v", m.sql)
	}
	if m.sql.p.Proj == nil {
		u.Warnf("no projection?? %#v", m.sql.p)
		return fmt.Errorf("no plan? %v", m.sql)
	}
	cols := m.sql.p.Proj.Columns
	colNames := make(map[string]int, len(cols))
	for i, col := range cols {
		colNames[col.As] = i
		//u.Debugf("%d col: %s %#v", i, col.As, col)
	}

	//u.Debugf("sqltopql:%p  resultreader:%p colnames? %v", m.sql, m, colNames)

	if m.response == nil {
		return fmt.Errorf("no response")
	}

	if !m.response.Success {
		return fmt.Errorf(m.response.ErrorMessage)
	}

	fromTime, err := time.Parse(shared.YMDHTimeFmt, m.sql.startDate)
	if err != nil {
		return err
	}
	toTime, err := time.Parse(shared.YMDHTimeFmt, m.sql.endDate)
	if err != nil {
		return err
	}

	if sql.CountStar() && len(m.sql.p.Stmt.JoinNodes()) == 0 {
		// select count(*)
		vals := make([]driver.Value, 1)
		ct := m.response.Count
		vals[0] = fmt.Sprintf("%d", ct)
		m.Vals = append(m.Vals, vals)
		//u.Debugf("was a select count(*) query %d", ct)
		msg := datasource.NewSqlDriverMessageMap(uint64(1), vals, colNames)
		//u.Debugf("In source Scanner iter %#v", msg)
		outCh <- msg
		return nil
	//} else if orig != nil && len(orig.From) > 1 {
	} else if len(m.sql.p.Stmt.JoinNodes()) > 0 {
		// This query must be part of a join.  Pass along the roaring bitmap results to the next
		// tasks in the process flow.
/*
		allTables := make([]string, len(orig.From))
		for i, v := range orig.From {
			allTables[i] = v.Name
		}
*/
		dataMap := make(map[string]interface{})
		dataMap["results"] = m.response.Results
		dataMap["fromTime"] = fromTime.UnixNano()
		dataMap["toTime"] = toTime.UnixNano()
		dataMap["table"] = m.sql.tbl.Name
		dataMap["isDriver"] = m.conn.IsDriverForJoin(m.sql.tbl.Name, m.sql.p.Stmt.JoinNodes()[0].String())
		dataMap["isDefaultWhere"] = false
		if m.sql.defaultWhere {
			dataMap["isDefaultWhere"] = true
		}
		msg := datasource.NewContextSimpleNative(dataMap)
		outCh <- msg
		return nil
	} else if cols[0].As == "@rownum" && len(cols) == 1 {
		outputRownumMessages(outCh, m.response.Results, m.limit, m.offset)
		return nil
	}

	if m.sql.isSum || m.sql.isAvg || m.sql.isMin || m.sql.isMax {
		projFields := []string{fmt.Sprintf("%s.%s", m.sql.tbl.Name, m.sql.aggField)}
		foundSet := make(map[string]*roaring64.Bitmap)
		foundSet[m.sql.tbl.Name] = m.response.Results
		proj, errx := core.NewProjection(m.conn, foundSet, nil, projFields, "",
			fromTime.UnixNano(), toTime.UnixNano(), nil, false)
		if errx != nil {
			return errx
		}
		vals := make([]driver.Value, 1)
		if m.sql.isSum || m.sql.isAvg {
			var sum int64
			var count uint64
			sum, count, errx = proj.Sum(m.sql.tbl.Name, m.sql.aggField)
			if errx != nil {
				return errx
			}
			vals[0] = fmt.Sprintf("%d", sum)
			if m.sql.isAvg && count != 0 {
				vals[0] = fmt.Sprintf("%d", sum/int64(count))
			}
		}
		if m.sql.isMin || m.sql.isMax {
			var minmax int64
			if m.sql.isMin {
				minmax, errx = proj.Min(m.sql.tbl.Name, m.sql.aggField)
			} else {
				minmax, errx = proj.Max(m.sql.tbl.Name, m.sql.aggField)
			}
			if errx != nil {
				return errx
			}
			table := m.sql.conn.TableBuffers[m.sql.tbl.Name].Table
			field, err := table.GetAttribute(m.sql.aggField)
			if err != nil {
				return err
			}
			switch shared.TypeFromString(field.Type) {
			case shared.Integer:
				vals[0] = fmt.Sprintf("%10d", minmax)
			case shared.Float:
				f := fmt.Sprintf("%%10.%df", field.Scale)
				vals[0] = fmt.Sprintf(f, float64(minmax)/math.Pow10(field.Scale))
			}
		}
		m.Vals = append(m.Vals, vals)
		//u.Debugf("was a select sum(*) query %d", ct)
		msg := datasource.NewSqlDriverMessageMap(uint64(1), vals, colNames)
		outCh <- msg
		return nil
	}

	table := m.conn.TableBuffers[m.sql.tbl.Name].Table
	if m.sql.isTopn {
		if _, err := table.GetAttribute(m.sql.aggField); err == nil {
			c1n := "topn_" + m.sql.aggField
			c2n := "topn_count"
			c3n := "topn_percent"
			cn := make(map[string]int, 3)
			cn[c1n] = 0
			cn[c2n] = 1
			cn[c3n] = 2
			projFields := []string{fmt.Sprintf("%s.%s", m.sql.tbl.Name, m.sql.aggField)}
			foundSet := make(map[string]*roaring64.Bitmap)
			foundSet[m.sql.tbl.Name] = m.response.Results
			proj, err3 := core.NewProjection(m.conn, foundSet, nil, projFields, "",
				fromTime.UnixNano(), toTime.UnixNano(), nil, false)
			if err3 != nil {
				return err3
			}
			rows, err4 := proj.Rank(m.sql.tbl.Name, m.sql.aggField, m.sql.topn)
			if err4 != nil {
				return err4
			}
			if len(rows) == 0 {
				return nil
			}
			for i, v := range rows {
				msg := datasource.NewSqlDriverMessageMap(uint64(i), v, cn)
				select {
				case <-sigChan:
					return nil
				case outCh <- msg:
					// continue
				}
			}
		}
		return nil
	}

	if m.limit != 0 {
		//m.query = m.query.Limit(m.limit)
	}

	if len(cols) == 0 {
		u.Errorf("WTF?  no cols? %v", cols)
	}

	// Made it this far, must be a simple, non-join projection
	//projFields := make([]string, 0)
	var projFields []string
	var rowCols map[string]int
	m.sql.p.Proj, colNames, rowCols, projFields, _, err = createProjection(orig, m.sql.tbl.Schema, "", 
		m.sql.whereProj)
	if err != nil {
		return err
	}
/*
	cols = m.sql.p.Proj.Columns
	for _, v := range cols {
		if v.As != "@rownum" {
			projFields = append(projFields, fmt.Sprintf("%s.%s", m.sql.tbl.Name, v.Name))
		}
	}
*/

	foundSet := make(map[string]*roaring64.Bitmap)
	foundSet[m.sql.tbl.Name] = m.response.Results
	proj, err3 := core.NewProjection(m.conn, foundSet, nil, projFields, "",
		fromTime.UnixNano(), toTime.UnixNano(), nil, false)
	if err3 != nil {
		return err3
	}

	isExport := false

	// Parallelize projection for SELECT ... INTO
	if orig.Into != nil {
		isExport = true
	}

	if err = outputProjection(outCh, sigChan, proj, colNames, rowCols, m.limit, m.offset, isExport,
		orig.Distinct, m.sql.p.Proj, orig.With); err != nil {
		return err
	}

	return nil
}
