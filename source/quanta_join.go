package source

// QuantaJoinMerge task implementation.

import (
	"database/sql/driver"
	"fmt"
	"sync"

	u "github.com/araddon/gou"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/exec"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/lex"
	"github.com/araddon/qlbridge/plan"
	"github.com/araddon/qlbridge/rel"
	"github.com/disney/quanta/core"
)

var (
	_ = u.EMPTY

	// Ensure that we implement the Task Runner interface
	_ exec.TaskRunner = (*JoinMerge)(nil)
)

// JoinMerge - Scans 2 source tasks for rows, calls server side join transpose
type JoinMerge struct {
	*exec.TaskBase
	leftStmt    *rel.SqlSource
	rightStmt   *rel.SqlSource
	ltask       exec.TaskRunner
	rtask       exec.TaskRunner
	colIndex    map[string]int
	allTables   []string
	driverTable string
	aliases     map[string]*rel.SqlSource
	isSubQuery  bool
}

// NewQuantaJoinMerge - Construct a QuantaJoinMerge task.
func NewQuantaJoinMerge(ctx *plan.Context, l, r exec.TaskRunner, p *plan.JoinMerge) exec.TaskRunner {

	m := &JoinMerge{
		TaskBase: exec.NewTaskBase(ctx),
		colIndex: p.ColIndex,
	}

	m.ltask = l
	m.rtask = r
	m.leftStmt = p.LeftFrom
	u.Debugf("LEFT STMT = %#v", p.LeftFrom)
	m.rightStmt = p.RightFrom
	u.Debugf("RIGHT STMT = %#v", p.RightFrom)
	orig := m.Ctx.Stmt.(*rel.SqlSelect)
	m.allTables = make([]string, len(orig.From))
	m.isSubQuery = len(orig.From) == 1
	m.aliases = make(map[string]*rel.SqlSource)
	for i, v := range orig.From {
		m.allTables[i] = v.Name
		alias := v.Alias
		if alias == "" {
			alias = v.Name
		}
		m.aliases[alias] = v
	}
	if _, ok := m.aliases[m.leftStmt.Name]; !ok {
		m.allTables = append(m.allTables, m.leftStmt.Name)
		alias := m.leftStmt.Alias
		if alias == "" {
			alias = m.leftStmt.Name
		}
		m.aliases[alias] = m.leftStmt
	}
	if _, ok := m.aliases[m.rightStmt.Name]; !ok {
		m.allTables = append(m.allTables, m.rightStmt.Name)
		alias := m.rightStmt.Alias
		if alias == "" {
			alias = m.rightStmt.Name
		}
		m.aliases[alias] = m.rightStmt
	}
	return m
}

// Run the task.
func (m *JoinMerge) Run() error {

	defer m.Ctx.Recover()
	defer close(m.MessageOut())

	outCh := m.MessageOut()

	leftIn := m.ltask.MessageOut()
	rightIn := m.rtask.MessageOut()

	var lresult *roaring64.Bitmap
	var rresult *roaring64.Bitmap
	foundSets := make(map[string]*roaring64.Bitmap)
	var foundSetLock sync.Mutex
	var rtable, ltable string
	var lisdefaultedpredicate, risdefaultedpredicate bool
	var fromTime, toTime int64
	orig := m.Ctx.Stmt.(*rel.SqlSelect)
	limit := orig.Limit
	if limit == 0 {
		limit = DefaultLimit
	}
	offset := orig.Offset

	wg := new(sync.WaitGroup)
	wg.Add(1)
	var fatalErr error
	go func() {
	loopExit:
		for {
			//u.Infof("In source Scanner msg %#v", msg)
			select {
			case <-m.SigChan():
				u.Debugf("got signal quit")
				wg.Done()
				wg.Done()
				return
			case msg, ok := <-leftIn:
				if !ok {
					//u.Debugf("NICE, got left shutdown")
					wg.Done()
					return
				}
				switch mt := msg.(type) {
				case *datasource.ContextSimple:
					// Process table input variables
					if x, ok := mt.Get("table"); ok {
						ltable = x.ToString()
					}
					if x, ok := mt.Get("isDriver"); ok {
						isDriver := x.Value().(bool)
						if isDriver {
							m.driverTable = ltable
						}
					}
					if x, ok := mt.Get("isDefaultWhere"); ok {
						isDefaultWhere := x.Value().(bool)
						if isDefaultWhere {
							lisdefaultedpredicate = true
						}
					}
					if x, ok := mt.Get("results"); ok {
						lresult = x.Value().(*roaring64.Bitmap)
					}
					foundSetLock.Lock()
					foundSets[ltable] = lresult
					if x, ok := mt.Get("foundSets"); ok {
						// fold in foundSets from previous step
						fs := x.Value().(map[string]*roaring64.Bitmap)
						for k, v := range fs {
							foundSets[k] = v
						}
					}
					foundSetLock.Unlock()
					if x, ok := mt.Get("fromTime"); ok {
						from := x.Value().(int64)
						if from != 0 {
							fromTime = from
						}
					}
					if x, ok := mt.Get("toTime"); ok {
						to := x.Value().(int64)
						if to != 0 {
							toTime = to
						}
					}
					//u.Debugf("%p LEFT INPUT RESULT = %d", m, lresult.GetCardinality())
				case *datasource.SqlDriverMessageMap:
					// fold in the results of previous JoinMerge
					lresult = mt.Values()[1].(*roaring64.Bitmap)
					wg.Done()
					break loopExit
				default:
					fatalErr = fmt.Errorf("to use QuantaJoin must use ContextSimple but got %T", msg)
					u.Errorf("%v - unrecognized msg %T", fatalErr, msg)
					close(m.TaskBase.SigChan())
					return
				}
			}
		}
	}()
	wg.Add(1)
	go func() {
		for {

			//u.Infof("In source Scanner iter %#v", item)
			select {
			case <-m.SigChan():
				u.Debugf("got quit signal join source 1")
				wg.Done()
				wg.Done()
				return
			case msg, ok := <-rightIn:
				if !ok {
					//u.Debugf("NICE, got right shutdown")
					wg.Done()
					return
				}
				switch mt := msg.(type) {
				case *datasource.ContextSimple:
					// Process join table variables
					if x, ok := mt.Get("results"); ok {
						rresult = x.Value().(*roaring64.Bitmap)
					}
					if x, ok := mt.Get("table"); ok {
						rtable = x.ToString()
					}
					foundSetLock.Lock()
					foundSets[rtable] = rresult
					foundSetLock.Unlock()
					if x, ok := mt.Get("fromTime"); ok {
						from := x.Value().(int64)
						if from != 0 {
							fromTime = from
						}
					}
					if x, ok := mt.Get("toTime"); ok {
						to := x.Value().(int64)
						if to != 0 {
							toTime = to
						}
					}
					if x, ok := mt.Get("isDriver"); ok {
						isDriver := x.Value().(bool)
						if isDriver {
							m.driverTable = rtable
						}
					}
					if x, ok := mt.Get("isDefaultWhere"); ok {
						isDefaultWhere := x.Value().(bool)
						if isDefaultWhere {
							risdefaultedpredicate = true
						}
					}
				default:
					fatalErr = fmt.Errorf("right msg input should receive ContextSimple but got %T", msg)
					u.Errorf("%v - unrecognized msg %T", fatalErr, msg)
					close(m.TaskBase.SigChan())
					return
				}
			}

		}
	}()
	wg.Wait()

	if fatalErr != nil {
		return fatalErr
	}
	//u.Info("leaving source scanner")
	//u.Debugf("%p LEFT CARD = %d", m, lresult.GetCardinality())
	//u.Debugf("%p RIGHT CARD = %d", m, rresult.GetCardinality())
	if lresult == nil && len(foundSets) < 2 {
		u.Errorf("foundSets len = %d", len(foundSets))
		panic("left result is nil!")
	}
	if rresult == nil {
		panic("right result is nil!")
	}

	haveAllResults := true
	for _, v := range m.allTables {
		if _, ok := foundSets[v]; !ok {
			haveAllResults = false
			break
		}
	}

	// If we don't have all results, pass foundSets (thus far) into the next join merge and exit
	if !haveAllResults {
		dataMap := make(map[string]interface{})
		dataMap["foundSets"] = foundSets
		if m.driverTable != "" {
			dataMap["table"] = m.driverTable
			dataMap["isDriver"] = true
			dataMap["fromTime"] = fromTime
			dataMap["toTime"] = toTime
		}
		msg := datasource.NewContextSimpleNative(dataMap)
		outCh <- msg
		return nil
	}
	if m.driverTable == "" {
		return fmt.Errorf("driver table not specified")
	}

	for k, v := range foundSets {
		u.Debugf("TABLE = %v, COUNT = %d", k, v.GetCardinality())
	}

	joinTypes := make(map[string]bool)
	negate := false
	innerJoin := false
	for _, v := range m.aliases {
		if v.JoinExpr != nil && v.JoinExpr.(*expr.BinaryNode).Operator.T == lex.TokenNE {
			negate = true
			u.Debugf("JOINEXPR = %#v, NEGATE = %v", v.JoinExpr, negate)
		}
		if v.JoinType == lex.TokenInner || (m.isSubQuery && !negate) {
			innerJoin = true
		}
	}
	joinTypes[m.driverTable] = innerJoin

	if orig.IsAggQuery() {
		nm := m.Ctx.Projection.Proj.Columns[0].As
		if nm == "count(*)" {
			rs, isOuter, err := m.callJoin(m.driverTable, foundSets, fromTime, toTime, negate)
			if err != nil {
				fatalErr = err
				return err
			}
			ct, _ := rs.Sum(rs.GetExistenceBitmap())
			// This test is necessary only if the foreign key can contain NULL values (which is the point of OUTER joins)
			// A corner case can exist if there are no predicates in which case there are cancelling AndNot operations
			//if isOuter && (!lisdefaultedpredicate || !risdefaultedpredicate) {
_ = lisdefaultedpredicate
_ = risdefaultedpredicate
			if isOuter {
				bm := roaring64.NewBitmap()
				for k, v := range foundSets {
					if k == m.driverTable {
						continue
					}
					bm = v.Clone()
					bm.AndNot(rs.GetExistenceBitmap())
					
				}
				ct += int64(bm.GetCardinality())
			}
			//u.Debugf("%p RESULT = %d", m, ct)
			vals := make([]driver.Value, 2)
			vals[0] = fmt.Sprintf("%d", ct)
			/* This is a sneaky way of passing the roaring results on to the next processing step
			 * If this is a single table join then the sqldriver ResultWriter jut ignores the second
			 * parameter (no colNames mapping).  For multi-table joins this value ends up as the
			 * left value intersected with the previous results.
			 */
			vals[1] = ct
			colNames := make(map[string]int, 1)
			colNames[nm] = 0
			outCh <- datasource.NewSqlDriverMessageMap(uint64(1), vals, colNames)
		}
		if nm == "@rownum" {
			//rs := roaring64.FastAnd(lresult, rresult)
			//outputRownumMessages(outCh, rs.GetExistenceBitmap(), limit, offset)
		}
	} else { //Assume a projection
		fp, cn, rn, projFields, joinFields, err := createProjection(orig, m.Ctx.Schema, m.driverTable, nil)
		if err != nil {
			return err
		}
		u.Debugf("FP = %#v, CN = %#v, RN = %#v, PROJF = %#v, JF = %#v", fp, cn, rn, projFields, joinFields)
		if m.isSubQuery {
			u.Debugf("LEN ALLTABLES = %d", len(m.allTables))
			_, cn, rn, projFields, joinFields, err = createFinalProjectionFromMaps(orig, m.aliases, m.allTables, 
					m.Ctx.Schema, m.driverTable)
			if err != nil {
				return err
			}
			u.Debugf("SUBQUERY CN = %#v, RN = %#v, PROJF = %#v, JF = %#v", cn, rn, projFields, joinFields)
		}
		val, found := m.Ctx.Session.Get(sessionPool)
		if !found {
			return fmt.Errorf("cannot obtain session pool from session")
		}
		sessionPool, ok := val.Value().(*core.SessionPool)
		if !ok {
			return fmt.Errorf("cannot cast session pool from stashed value")
		}
		
		con, err := sessionPool.Borrow(m.driverTable)
		if err != nil {
			return fmt.Errorf("connot borrow a connection from the pool.");
		}
		defer sessionPool.Return(m.driverTable, con)
		// driver table found set may have been reduced by join results
		proj, err2 := core.NewProjection(con, foundSets, joinFields, projFields, m.driverTable, m.leftStmt.Name,
			fromTime, toTime, joinTypes, negate)
		if err2 != nil {
			return err2
		}
		isExport := false
		u.Debugf("DRIVERTABLE = %v, NEGATE PROJECTION = %v", m.driverTable, negate)


		// Parallelize projection for SELECT ... INTO
		if orig.Into != nil {
			isExport = true
		}

		if err = outputProjection(outCh, m.SigChan(), proj, cn, rn, limit, offset, isExport,
			orig.Distinct, fp, orig.With); err != nil {
			return err
		}
	}
	return nil
}


func (m *JoinMerge) callJoin(table string, foundSets map[string]*roaring64.Bitmap,
	fromTime, toTime int64, negate bool) (*roaring64.BSI, bool, error) {

	val, found := m.Ctx.Session.Get(sessionPool)
	if !found {
		return nil, false, fmt.Errorf("cannot obtain session pool from session")
	}
	sessionPool, ok := val.Value().(*core.SessionPool)
	if !ok {
		return nil, false, fmt.Errorf("cannot cast session pool from stashed value")
	}
	con, err := sessionPool.Borrow(table)
	if err != nil {
		return nil, false, fmt.Errorf("connot borrow a connection from the pool.");
	}
	defer sessionPool.Return(table, con)

	joinCols := make([]string, 0)
	filterSetArray := make([]*roaring64.Bitmap, 0)
	foundSet := foundSets[m.driverTable]
	isOuter := false
	// Should there be a 1:1 correspondence between joinCols and filterSetArray?  Bug?
	for _, v := range m.aliases {
		for _, y := range v.JoinNodes() {
			if v.Name == m.driverTable {
				joinCols = append(joinCols, y.String())
			} else {
				filterSetArray = append(filterSetArray, foundSets[v.Name])
			}
			isOuter = !isOuter && v.JoinType == lex.TokenOuter
		}
	}

	u.Debugf("TABLE %s, JOINCOLS = %#v, FS = %d, FILTER = %#v, NEGATE = %v", table, joinCols, 
		foundSet.GetCardinality(), filterSetArray, negate)

	rs, err := con.BitIndex.Join(table, joinCols, fromTime, toTime, foundSet, filterSetArray, false)
	if err != nil {
		return nil, false, err
	}
	if negate {
		childTransposed := rs.GetExistenceBitmap()
		parent := filterSetArray[0]
		driver := roaring64.AndNot(parent, childTransposed)
		rsa := roaring64.NewBSI(0, 1)
		var i uint64
		var da []uint64
		da = driver.ToArray()
		for i = 0; i < uint64(len(da)); i++ {
			rsa.SetValue(i, int64(1))
		}
		rs = rsa
		u.Debugf("PARENT = %d, CHILD = %d,  FDIFF %d",  parent.GetCardinality(), childTransposed.GetCardinality(), 
			driver.GetCardinality())
	}
	return rs, isOuter, nil
}

