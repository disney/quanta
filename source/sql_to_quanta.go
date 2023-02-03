package source

// SQLToQuanta query langage adapter/translator supports SQL query language.

import (
	"database/sql/driver"
	"fmt"
	"strings"
	"time"

	u "github.com/araddon/gou"
	"golang.org/x/net/context"

	"github.com/araddon/dateparse"
	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/exec"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/lex"
	"github.com/araddon/qlbridge/plan"
	"github.com/araddon/qlbridge/rel"
	"github.com/araddon/qlbridge/schema"
	"github.com/araddon/qlbridge/value"
	"github.com/araddon/qlbridge/vm"
	"github.com/disney/quanta/core"
	"github.com/disney/quanta/rbac"
	"github.com/disney/quanta/shared"
)

var (
	// DefaultLimit is page limit
	DefaultLimit = 5000

	// Ensure we implment appropriate interfaces
	_ schema.Conn           = (*SQLToQuanta)(nil)
	_ plan.SourcePlanner    = (*SQLToQuanta)(nil)
	_ exec.ExecutorSource   = (*SQLToQuanta)(nil)
	_ schema.ConnMutation   = (*SQLToQuanta)(nil)
	_ schema.ConnPatchWhere = (*SQLToQuanta)(nil)
)

const (
	servicePort  = "SERVICE_PORT"
	basePath     = "BASE_PATH"
	metadataPath = "METADATA_PATH"
	userIDKey    = "@userid"
)

// SQLToQuanta Convert a Sql Query to a Quanta query
// - responsible for pushing down as much logic to Quanta as possible
// - dialect translator
type SQLToQuanta struct {
	*exec.TaskBase
	resp           *ResultReader
	tbl            *schema.Table
	p              *plan.Source
	sel            *rel.SqlSelect
	stmt           rel.SqlStatement
	schema         *schema.Schema
	limit          int
	offset         int
	hasMultiValue  bool // Multi-Value vs Single-Value aggs
	hasSingleValue bool // single value agg
	isSum          bool
	isAvg          bool
	isMin          bool
	isMax          bool
	isTopn         bool
	topn           int
	aggField       string
	startDate      string
	endDate        string
	s              *QuantaSource
	conn           *core.Session
	q              *shared.BitmapQuery
	defaultWhere   bool
	needsPolyFill  bool // polyfill?
	funcAliases    map[string]struct{}
	whereProj      map[string]*core.Attribute
}

// NewSQLToQuanta - Construct a new SQLToQuanta query translator.
func NewSQLToQuanta(s *QuantaSource, t *schema.Table) *SQLToQuanta {
	m := &SQLToQuanta{
		tbl:    t,
		schema: t.Schema,
		s:      s,
	}
	m.funcAliases = make(map[string]struct{})
	m.whereProj = make(map[string]*core.Attribute)
	return m
}

// ResolveField - Resolve a attribute by name.
func (m *SQLToQuanta) ResolveField(name string) (field *core.Attribute, isBSI bool, err error) {

	if name == "@rownum" {
		return
	}
	isBSI = false
	table := m.conn.TableBuffers[m.tbl.Name].Table
	field, err = table.GetAttribute(name)
	if err != nil {
		return
	}
	if core.MapperTypeFromString(field.MappingStrategy).IsBSI() {
		isBSI = true
	}
	return
}

// WalkSourceSelect An interface implemented by this session allowing the planner
// to push down as much logic into this source as possible
func (m *SQLToQuanta) WalkSourceSelect(planner plan.Planner, p *plan.Source) (plan.Task, error) {

	//u.Debugf("VisitSourceSelect(): %s", p.Stmt)

	//u.Debugf("WalkSourceSelect %p", m)
	p.Conn = m

	if len(p.Custom) == 0 {
		p.Custom = make(u.JsonHelper)
	}

	var err error
	m.conn, err = m.s.sessionPool.Borrow(m.tbl.Name)
	if err != nil {
		return nil, fmt.Errorf("Error opening Quanta session %v", err)
	}
	defer m.s.sessionPool.Return(m.tbl.Name, m.conn)

	// Create a session if one doesn't exist and add the join strategy indicator
	// Add indicators to create no-op tasks for where clauses and groupby
	sessionMap := make(map[string]interface{})
	sessionMap[servicePort] = m.s.sessionPool.AppHost.ServicePort
	sessionMap[basePath] = m.conn.BasePath
	sessionMap[exec.GROUPBY_MAKER] = func(ctx *plan.Context, p *plan.GroupBy) exec.TaskRunner {
		return NewNopTask(ctx)
	}
	sessionMap[exec.PROJECTION_MAKER] = func(ctx *plan.Context, p *plan.Projection) exec.TaskRunner {
		return NewQuantaProjection(ctx)
	}
	sessionMap[exec.JOINMERGE_MAKER] = NewQuantaJoinMerge
	if p.Context().Session == nil {
		session := datasource.NewContextSimpleNative(sessionMap)
		p.Context().Session = session
	} else {
		for k, v := range sessionMap {
			sk := SchemaInfoString{k: k}
			p.Context().Session.Put(sk, nil, value.NewValue(v))
		}
	}
	userID, ok := p.Context().Session.Get(userIDKey)
	if !ok {
		return nil, fmt.Errorf("User ID (%s) not set for session", userIDKey)
	}
	authCtx, err2 := rbac.NewAuthContext(m.conn.KVStore, userID.ToString(), false)
	if err2 != nil {
		return nil, fmt.Errorf("RBAC error - %v", err2)
	}
	u.Debugf("RBAC AuthContext created, USER ID = %v", userID.ToString())
	if ok, err2 := authCtx.IsAuthorized(rbac.ViewDatabase, m.schema.Name); !ok {
		return nil, fmt.Errorf("ViewDatabase not authorized on schema %s - %v", m.schema.Name, err2)
	}

	m.TaskBase = exec.NewTaskBase(p.Context())

	p.SourceExec = true
	m.q = shared.NewBitmapQuery()
	frag := m.q.NewQueryFragment()

	p.Complete = true
	if m.needsPolyFill {
		p.Custom["poly_fill"] = true
		//u.Warnf("%p  need to signal poly-fill", m)
	//} else {
	//	p.Complete = true
	}
	m.p = p
	req := p.Stmt.Source

	m.sel = req

	// If the query is a join then it will be split into multiple queries, the original SQL
	// is in p.Context().Stmt.  Verify that the original join syntax was correct.
	if orig, ok := p.Context().Stmt.(*rel.SqlSelect); ok {
		if len(orig.From) > 1 {
			foundCriteria := false
			foundParentRelation := false
			tables := make([]string, 0)
			for _, x := range orig.From {
				table, err := p.Context().Schema.Table(x.Name)
				if err != nil {
					return nil, fmt.Errorf("invalid table %s in join criteria [%v]", x.Name, x)
				}
				tables = append(tables, x.Name)
				// Validate join nodes
				for _, y := range x.JoinNodes() {
					foundCriteria = true
					field, ok := table.FieldMap[y.String()]
					if !ok {
						return nil, fmt.Errorf("invalid field %s in join criteria [%v]", y.String(), x)
					}
					if field.Extra == "ParentRelation" {
						foundParentRelation = true
						continue // Its a relation so we're good
					}
					if field.Key != "-" {
						continue // Its part of a PK or SK so we're good
					}
					return nil, fmt.Errorf("join field %s is not a relation", y.String())
				}
			}
			if !foundCriteria {
				return nil, fmt.Errorf("join criteria missing (ON clause)")
			}
			if !foundParentRelation && m.conn.IsDriverForTables(tables) {
				return nil, fmt.Errorf("join criteria missing (no relation )")
			}
		}
	}

	m.offset = req.Offset
	m.limit = req.Limit

	table := m.conn.TableBuffers[m.tbl.Name].Table

	if req.Where == nil || req.Where != nil && req.Where.Source != nil {
		pka, _ := table.GetPrimaryKeyInfo()
		predicate := fmt.Sprintf("%s != NULL", pka[0].FieldName)
		defaultWhere, _ := expr.ParseExpression(predicate)
		req.Where = rel.NewSqlWhere(defaultWhere)
		m.defaultWhere = true
	}

	// Evaluate the Select columns make sure we can pass them down or polyfill
	err = m.walkSelectList(frag)
	if err != nil {
		u.Warnf("Could Not evaluate Columns/Aggs %s %v", req.Columns.String(), err)
		return nil, err
	}

	// if Where.Source is not nil then it is a subquery where clause that is walked separately via the planner
	if req.Where != nil && req.Where.Source == nil {
		_, err = m.walkNode(req.Where.Expr, frag)
		if err != nil {
			u.Warnf("Could Not evaluate Where Node %s %v", req.Where.Expr.String(), err)
			return nil, err
		}
	}

	/*
	   if len(req.GroupBy) > 0 {
	       err = m.walkGroupBy()
	       if err != nil {
	           u.Warnf("Could Not evaluate GroupBys %s %v", req.GroupBy.String(), err)
	           return nil, err
	       }
	   }

	   u.Debugf("OrderBy? %v", len(m.sel.OrderBy))
	   if len(m.sel.OrderBy) > 0 {
	       m.sort = make([]bson.M, len(m.sel.OrderBy))
	       for i, col := range m.sel.OrderBy {
	           // We really need to look at any funcs?   walk this out
	           switch col.Order {
	           case "ASC":
	               m.sort[i] = bson.M{col.As: 1}
	           case "DESC":
	               m.sort[i] = bson.M{col.As: -1}
	           default:
	               // default sorder order = ?
	               m.sort[i] = bson.M{col.As: -1}
	           }
	       }
	   }
	*/

	if m.p.Complete {
		sessionMap[exec.WHERE_MAKER] = func(ctx *plan.Context, p *plan.Where) exec.TaskRunner {
			return NewNopTask(ctx)
		}
		v := sessionMap[exec.WHERE_MAKER]
		sk := SchemaInfoString{k: exec.WHERE_MAKER}
		p.Context().Session.Put(sk, nil, value.NewValue(v))
	} else {
		dm := make(map[string]value.Value)
		dm[exec.WHERE_MAKER] = value.NilValueVal
		p.Context().Session.Delete(dm)
		//p.Context().Projection.Proj.Final = false
		// Make sure identities in WHERE are in the result set
		ids := expr.FindAllIdentities(req.Where.Expr)
		for _, n := range ids {
			var tableName, fieldName string
			l, r, isLr := n.LeftRight()
			if isLr {
				tableName = l
				fieldName = r
			} else {
				fieldName = n.Text
				tableName = m.tbl.Name
			}
			table := m.conn.TableBuffers[tableName].Table
			if attr, err := table.GetAttribute(fieldName); err == nil {
				f := fmt.Sprintf("%s.%s", tableName, fieldName)
				m.whereProj[f] = attr
			}
		}
	}

	if m.startDate == "" || table.TimeQuantumType == "" {
		m.startDate = time.Unix(0, 0).Format(shared.YMDHTimeFmt)
	}
	if table.TimeQuantumType != "" && m.endDate == "" {
		end := time.Now().AddDate(0, 0, 1)
		m.endDate = end.Format(shared.YMDHTimeFmt)
	}
	if table.TimeQuantumType == "" {
		m.endDate = time.Unix(0, 0).Format(shared.YMDHTimeFmt)
	}
	m.q.FromTime = m.startDate
	m.q.ToTime = m.endDate

	if m.q.GetRootIndex() == "" && len(m.funcAliases) > 0 {
		// root index is empty because there was no predicate for the backend.  Create a new default query.
		if m.limit == 0 {
			return nil, fmt.Errorf("If there is a post process filter predicate then you must specify limit")
		}
		pka, _ := table.GetPrimaryKeyInfo()
		m.q = shared.NewBitmapQuery()
		m.q.ToTime = m.endDate
		p := m.q.NewQueryFragment()
		p.SetNullPredicate(m.tbl.Name, pka[0].FieldName)
		p.Operation = "DIFFERENCE"
		p.Negate = true
		m.q.AddFragment(p)
	}

	return nil, nil
}

// Walk() an expression, and its logic to create an appropriately
// nested structure for quanta queries if possible.
//
// - if can't express logic we need to allow qlbridge to poly-fill
//
func (m *SQLToQuanta) walkNode(cur expr.Node, q *shared.QueryFragment) (value.Value, error) {
	//u.Debugf("WalkNode: %#v", cur)
	switch curNode := cur.(type) {
	case *expr.NumberNode, *expr.StringNode:
		nodeVal, ok := vm.Eval(nil, cur)
		if !ok {
			u.Warnf("not ok %v", cur)
			return nil, fmt.Errorf("could not evaluate: %v", cur.String())
		}
		u.Infof("nodeval? %v", nodeVal)
		return nodeVal, nil
		// What do we do here?
	case *expr.BinaryNode:
		return m.walkFilterBinary(curNode, q)
	case *expr.TriNode: // Between
		return m.walkFilterTri(curNode, q)
	case *expr.UnaryNode:
		return m.walkFilterUnary(curNode, q)
	case *expr.FuncNode:
		ctx := datasource.NewContextSimpleNative(map[string]interface{}{"q": q, "m": m})
		val, ok := vm.Eval(ctx, curNode)
		if !ok {
			return nil, fmt.Errorf("function evaluation failed")
		}
		return val, nil
		//return m.walkFilterFunc(curNode, q)
	case *expr.IdentityNode:
		u.Warnf("we are trying to project?   %v", curNode.String())
		return value.NewStringValue(curNode.String()), nil
	case *expr.ArrayNode:
		return m.walkArrayNode(curNode, q)
	default:
		u.Errorf("unrecognized T:%T  %v", cur, cur)
		panic("Unrecognized node type")
	}
	return nil, nil
}

// Tri Nodes expressions:
//
//     <expression> [NOT] BETWEEN <expression> AND <expression>
//
func (m *SQLToQuanta) walkFilterTri(node *expr.TriNode, q *shared.QueryFragment) (value.Value, error) {

	if node.Negated() {
		q.Operation = "DIFFERENCE"
	}
	arg1val, aok, _ := m.eval(node.Args[0])
	if !aok {
		return nil, fmt.Errorf("could not evaluate args: %v", node.String())
	}

	arg2val, bok := vm.Eval(nil, node.Args[1])
	arg3val, cok := vm.Eval(nil, node.Args[2])
	if !bok || !cok {
		return nil, fmt.Errorf("could not evaluate args: %v", node.String())
	}
	switch node.Operator.T {
	case lex.TokenBetween:
		//u.Warnf("between? %T", arg2val.Value())
		nm := arg1val.ToString()
		if nm == "@timestamp" {
			m.startDate = arg2val.ToString()
			m.endDate = arg3val.ToString()
			return nil, nil
		}
		fr, ft, err := m.ResolveField(nm)
		if !ft || err != nil {
			if ft {
				err := fmt.Errorf("BETWEEN error %v", err)
				u.Errorf(err.Error())
				return nil, err
			}
			err := fmt.Errorf("BETWEEN not supported for non-range field '%s'", nm)
			u.Errorf(err.Error())
			return nil, err
		}
		/*
		   if !arg2val.Type().IsNumeric() && arg2val.Type() != value.TimeType {
		       err := fmt.Errorf("BETWEEN value compared to '%s' must be numeric or date It is %T ", nm, arg2val)
		       u.Errorf(err.Error())
		       return nil, err
		   }
		   if !arg3val.Type().IsNumeric() && arg3val.Type() != value.TimeType{
		       err := fmt.Errorf("BETWEEN value compared to '%s' must be numeric or date. It is %T ", nm, arg3val)
		       u.Errorf(err.Error())
		       return nil, err
		   }
		*/
		tbuf, found := m.conn.TableBuffers[fr.Parent.Name]
		if !found {
			err := fmt.Errorf("table %s not open", fr.Parent.Name)
			u.Errorf(err.Error())
			return nil, err
		}
		if tbuf.PKAttributes[0].FieldName == fr.FieldName {
			// If this field is the PK and the time partitions are not set then use these values and set them.
			loc, _ := time.LoadLocation("Local")
			if m.startDate == "" && tbuf.Table.TimeQuantumType != "" {
				start, err := dateparse.ParseIn(arg2val.ToString(), loc)
				if err != nil {
					err := fmt.Errorf("cannot parse start value '%v' - %v", arg2val, err)
					u.Errorf(err.Error())
					return nil, err
				}
				if tbuf.Table.TimeQuantumType == "YMDH" {
					m.startDate = start.Format(shared.YMDHTimeFmt)
				} else {
					m.startDate = start.Format(shared.YMDTimeFmt)
				}
			}
			if m.endDate == "" && tbuf.Table.TimeQuantumType != "" {
				end, err := dateparse.ParseIn(arg3val.ToString(), loc)
				if err != nil {
					err := fmt.Errorf("cannot parse end value '%v' - %v", arg3val, err)
					u.Errorf(err.Error())
					return nil, err
				}
				end = end.AddDate(0, 0, 1)
				if tbuf.Table.TimeQuantumType == "YMDH" {
					m.endDate = end.Format(shared.YMDHTimeFmt)
				} else {
					m.endDate = end.Format(shared.YMDTimeFmt)
				}
			}
		}

		if fr.Type == "Float" {
			if arg2val.Type() == value.StringType {
				arg2val = arg2val.(value.StringValue).NumberValue()
				if arg2val.Err() {
					err := fmt.Errorf("expecting a floating point value in first argument for %s", nm)
					u.Errorf(err.Error())
					return nil, err
				}
			}
			if arg3val.Type() == value.StringType {
				arg3val = arg3val.(value.StringValue).NumberValue()
				if arg3val.Err() {
					err := fmt.Errorf("expecting a floating point value in second argument for %s", nm)
					u.Errorf(err.Error())
					return nil, err
				}
			}
		}

		leftval, err1 := m.conn.MapValue(m.tbl.Name, nm, arg2val.Value(), false)
		rightval, err2 := m.conn.MapValue(m.tbl.Name, nm, arg3val.Value(), false)
		if err1 == nil && err2 == nil {
			q.SetBSIRangePredicate(fr.Parent.Name, fr.FieldName, int64(leftval), int64(rightval))
		} else if err1 != nil {
			err := fmt.Errorf("BETWEEN cannot map left value %v in field '%s' - %v", arg2val.Value(), nm, err1)
			u.Errorf(err.Error())
			return nil, err
		} else {
			err := fmt.Errorf("BETWEEN cannot map right value %v in field '%s' %v", arg3val.Value(), nm, err2)
			u.Errorf(err.Error())
			return nil, err
		}
		q.Query.AddFragment(q)
	default:
		return nil, fmt.Errorf("not implemented")
	}

	if q != nil {
		return nil, nil
	}
	return nil, fmt.Errorf("not implemented")
}

// Array Nodes expressions:
//
//    year IN (1990,1992)  =>
//
func (m *SQLToQuanta) walkArrayNode(node *expr.ArrayNode, q *shared.QueryFragment) (value.Value, error) {

	terms := make([]interface{}, 0, len(node.Args))
	for _, arg := range node.Args {
		// Do we eval here?
		v, ok := vm.Eval(nil, arg)
		if ok {
			u.Debugf("in? %T %v value=%v", v, v, v.Value())
			terms = append(terms, v.Value())
		} else {
			u.Warnf("could not evaluate arg: %v", arg)
		}
	}
	if q != nil {
		//u.Debug(string(u.JsonHelper(*q).PrettyJson()))
		return nil, nil
	}
	return nil, fmt.Errorf("Uknown Error")
}

// Binary Node:   operations for >, >=, <, <=, =, !=, AND, OR, Like, IN
//
//    x = y             =>   db.users.find({field: {"$eq": value}})
//    x != y            =>   db.inventory.find( { qty: { $ne: 20 } } )
//
//    x like "list%"    =>   db.users.find( { user_id: /^list/ } )
//    x like "%list%"   =>   db.users.find( { user_id: /bc/ } )
//    x IN [a,b,c]      =>   db.users.find( { user_id: {"$in":[a,b,c] } } )
//
func (m *SQLToQuanta) walkFilterBinary(node *expr.BinaryNode, q *shared.QueryFragment) (value.Value, error) {

	// If we have to recurse deeper for AND, OR operators
	switch node.Operator.T {
	case lex.TokenLogicAnd, lex.TokenLogicOr:
		rhq := q.Query.NewQueryFragment()
		if node.Operator.T == lex.TokenLogicAnd {
			//u.Debugf("AND - LEFT (%T) [%v] RIGHT (%T) [%v] Nested = %v", node.Args[0], node.Args[0], node.Args[1], node.Args[1], node.Paren)
			if q.Operation == "" {
				q.Operation = "INTERSECT"
			}
			rhq.Operation = "INTERSECT"
		} else {
			//u.Debugf("OR - LEFT (%T) [%v] RIGHT (%T) [%v] Nested = %v", node.Args[0], node.Args[0], node.Args[1], node.Args[1], node.Paren)
			if q.Operation == "" {
				q.Operation = "UNION"
				q.ORContext = true
			}
			rhq.Operation = "UNION"
		}
		//if node.Paren {
		//   newFrag := q.Query.NewQueryFragment()
		//   newFrag.Operation = q.Operation
		//   q.SetParent(newFrag)
		//   rhq.SetParent(newFrag)
		//   q.Query.PushLevel(q)
		//}
		_, err := m.walkNode(node.Args[0], q)
		_, err2 := m.walkNode(node.Args[1], rhq)
		if err != nil || err2 != nil {
			u.Errorf("could not get children nodes: %v %v %v", err, err2, node)
			return nil, fmt.Errorf("could not evaluate: %v %v %v", node.String(), err, err2)
		}
		if q.Index != "" && !q.Added {
			//u.Debugf("LHQ = %p", q)
			q.Query.AddFragment(q)
		}
		if rhq.Index != "" && !rhq.Added {
			//u.Debugf("RHQ = %p", rhq)
			q.Query.AddFragment(rhq)
		}
		//if node.Paren {
		//   q.Query.PopLevel()
		//}
		return nil, nil
	}

	lhval, lhok, isLident := m.eval(node.Args[0])
	rhval, rhok, isRident := m.eval(node.Args[1])
	_, rhisnull := node.Args[1].(*expr.NullNode)
	if !lhok {
		// if the lvalue is a function that takes identity nodes then a "WHERE" processor is required.
		if m.p.Complete && node.Args[0].NodeType() == "Func" {
			for _, x := range node.Args[0].(*expr.FuncNode).Args {
				if x.NodeType() == "Identity" {
					m.p.Complete = false
					break
				}
			}
		}
		if m.p.Complete {
			// if the lvalue references a function in the select list then post process "WHERE"
			if n, isId := node.Args[0].(*expr.IdentityNode); isId {
				if _, ok := m.funcAliases[n.Text]; ok {
					m.p.Complete = false
				}
			}
		}
		if m.p.Complete {
			u.Warnf("not ok: %v  l:%v  r:%v", node, lhval, rhval)
			return nil, fmt.Errorf("could not evaluate left arg: %v", node.String())
		}
	}
	if !rhok && !rhisnull {
		exprNode, _ := expr.ParseExpression(node.Args[1].String())
		val, ok := vm.Eval(nil, exprNode)
		if !ok {
			u.Warnf("not ok: %v  l:%v  r:%v", node, lhval, rhval)
			return nil, fmt.Errorf("could not evaluate: %v", node.String())
		}
		rhval = val
	}
	if isLident && isRident {
		// comparison of left/right isn't possible with Quanta
		// db.T.find( { $where : "this.Grade1 > this.Grade2" } );
		// u.Infof("identents?  %v %v  %v", lhval, rhval, node)
		return nil, fmt.Errorf("right hand argument %v cannot be field name", rhval)
	}
	//u.Debugf("walkBinary: %v  l:%v  r:%v  %T  %T", node, lhval, rhval, lhval, rhval)
	switch node.Operator.T {
	case lex.TokenEqual, lex.TokenEqualEqual, lex.TokenNE:
		if node.Operator.T == lex.TokenNE {
			q.Operation = "DIFFERENCE"
			q.Negate = true
		}
		// The $eq expression is equivalent to { field: <value> }.
		if lhval != nil && (rhval != nil || rhisnull) {
			// Add a Bitmap or BSI EQ query to tree lhval is frame, rhval is rowID (mapped)
			if f, isBSI, err := m.ResolveField(lhval.ToString()); err == nil {
				if rhisnull {
					q.Operation = "DIFFERENCE" // Avoid conversion to UNION, corrected server side.
					q.SetNullPredicate(f.Parent.Name, f.FieldName)
				} else {
					rowID, err := m.conn.MapValue(m.tbl.Name, lhval.ToString(), rhval.Value(), false)
					if err != nil {
						u.Warnf("not ok: %v  l:%v  r:%v", node, lhval, rhval)
						return nil, err
					}
					if isBSI {
						q.SetBSIPredicate(f.Parent.Name, f.FieldName, "EQ", int64(rowID))
						tbuf, found := m.conn.TableBuffers[f.Parent.Name]
						if !found {
							err := fmt.Errorf("table %s not open", f.Parent.Name)
							u.Errorf(err.Error())
							return nil, err
						}
						if tbuf.PKAttributes[0].FieldName == f.FieldName {
							loc, _ := time.LoadLocation("Local")
							if tbuf.Table.TimeQuantumType != "" && (m.startDate == "" || m.endDate == "") {
								ts, err := dateparse.ParseIn(rhval.ToString(), loc)
								if err != nil {
									err := fmt.Errorf("cannot parse value '%v' - %v", rhval, err)
									u.Errorf(err.Error())
									return nil, err
								}
								if m.startDate == "" {
									m.startDate = ts.Format(shared.YMDHTimeFmt)
								}
								if m.endDate == "" {
									end := ts.AddDate(0, 0, 1)
									m.endDate = end.Format(shared.YMDHTimeFmt)
								}
							}
						}
					} else {
						q.SetBitmapPredicate(f.Parent.Name, f.FieldName, rowID)
					}
				}
			} else {
				u.Warnf("not ok: %v  l:%v  r:%v", node, lhval, rhval)
				return nil, err
			}
		}
	case lex.TokenLE:
		return nil, m.handleBSI("LE", lhval, rhval, q)
	case lex.TokenLT:
		return nil, m.handleBSI("LT", lhval, rhval, q)
	case lex.TokenGE:
		return nil, m.handleBSI("GE", lhval, rhval, q)
	case lex.TokenGT:
		return nil, m.handleBSI("GT", lhval, rhval, q)
	case lex.TokenLike:
		nm := lhval.ToString()
		fr, ft, err := m.ResolveField(nm)
		if !ft || err != nil {
			if err != nil {
				u.Warnf("!= error %v", err)
				return nil, err
			}
			err := fmt.Errorf("LIKE operator not supported for non-range field '%s'", nm)
			u.Errorf(err.Error())
			return nil, err
		}
		q.Index = fr.Parent.Name
		q.Field = fr.FieldName
		if q.Operation == "UNION" {
			q.Operation = "LIKE_UNION"
		} else {
			q.Operation = "LIKE_INTERSECT"
		}
		q.Search = rhval.ToString()
		q.Query.AddFragment(q)
		return nil, nil
	case lex.TokenIN:
		isNegate := q.Negate
		switch vt := rhval.(type) {
		case value.SliceValue:
			nm := lhval.ToString()
			fr, ft, err := m.ResolveField(nm)
			if !ft || err != nil {
				if err != nil {
					u.Warnf("!= error %v", err)
					return nil, err
				}
				firstTime := true
				for _, v := range vt.Values() {
					rowID, err := m.conn.MapValue(m.tbl.Name, nm, v, false)
					if err != nil {
						u.Warnf("not ok: %v  l:%v  r:%v", node, nm, v)
						return nil, err
					}
					if firstTime {
						firstTime = false
						q.SetBitmapPredicate(fr.Parent.Name, fr.FieldName, rowID)
						if isNegate {
							q.Operation = "DIFFERENCE"
						} else {
							q.Operation = "UNION"
						}
						q.Query.AddFragment(q)
					} else {
						f := q.Query.NewQueryFragment()
						f.Negate = isNegate
						f.SetBitmapPredicate(fr.Parent.Name, fr.FieldName, rowID)
						if isNegate {
							f.Operation = "DIFFERENCE"
						} else {
							f.Operation = "UNION"
						}
						q.Query.AddFragment(f)
					}
				}
			} else {
				values := make([]int64, 0)
				for _, v := range vt.Values() {
					rowID, err := m.conn.MapValue(m.tbl.Name, nm, v, false)
					if err != nil {
						u.Warnf("not ok: %v  l:%v  r:%v", node, nm, v)
						return nil, err
					}
					values = append(values, int64(rowID))
				}
				if isNegate {
					q.Operation = "DIFFERENCE"
				}
				q.SetBSIBatchEQPredicate(fr.Parent.Name, fr.FieldName, values)
				q.Query.AddFragment(q)
			}
		default:
			return nil, fmt.Errorf("not implemented type %#v", rhval)
		}

	default:
		return nil, fmt.Errorf("not implemented: %v", node.Operator)
	}
	if q != nil {
		if !q.Added {
			// Must been a simple single predicate query
			q.Query.AddFragment(q)
		}
		return nil, nil
	}
	return nil, fmt.Errorf("not implemented %v", node.String())
}

func (m *SQLToQuanta) handleBSI(op string, lhval, rhval value.Value, q *shared.QueryFragment) error {

	if lhval == nil {  // silently ignore
		return nil
	}

	nm := lhval.ToString()
	fr, ft, err := m.ResolveField(nm)
	if !ft || err != nil {
		if err != nil {
			u.Warnf("!= error %v", err)
			return err
		}
		err := fmt.Errorf("operation %s not supported for non-range field '%s'", op, nm)
		u.Errorf(err.Error())
		return err
	}
	if fr.Type == "Float" {
		if rhval.Type() == value.StringType {
			rhval = rhval.(value.StringValue).NumberValue()
			if rhval.Err() {
				err := fmt.Errorf("expecting a floating point value for %s", nm)
				u.Errorf(err.Error())
				return err
			}
		}
	}
	if mv, err := m.conn.MapValue(m.tbl.Name, nm, rhval.Value(), false); err == nil {
		q.SetBSIPredicate(fr.Parent.Name, fr.FieldName, op, int64(mv))
		tbuf, found := m.conn.TableBuffers[fr.Parent.Name]
		if !found {
			err := fmt.Errorf("table %s not open", fr.Parent.Name)
			u.Errorf(err.Error())
			return err
		}
		if tbuf.PKAttributes[0].FieldName == fr.FieldName {
			loc, _ := time.LoadLocation("Local")
			if tbuf.Table.TimeQuantumType != "" && (m.startDate == "" || m.endDate == "") {
				ts, err := dateparse.ParseIn(rhval.ToString(), loc)
				if err != nil {
					err := fmt.Errorf("cannot parse value '%v' - %v", rhval, err)
					u.Errorf(err.Error())
					return err
				}
				if m.startDate == "" && (op == "GE" || op == "GT") {
					m.startDate = ts.Format(shared.YMDHTimeFmt)
				}
				if m.endDate == "" && (op == "LE" || op == "LT") {
					end := ts.AddDate(0, 0, 1)
					m.endDate = end.Format(shared.YMDHTimeFmt)
				}
			}
		}
	} else {
		err := fmt.Errorf("operation %s,  cannot map value %v in field '%s'", op, rhval.Value(), nm)
		u.Errorf(err.Error())
		return err
	}
	q.Query.AddFragment(q)
	return nil
}

func (m *SQLToQuanta) walkFilterUnary(node *expr.UnaryNode, q *shared.QueryFragment) (value.Value, error) {

	switch node.Operator.T {
	case lex.TokenNegate: // NOT keyword
		q.Negate = true
		switch curNode := node.Arg.(type) {
		case *expr.BinaryNode:
			return m.walkFilterBinary(curNode, q)
		case *expr.TriNode:
			return m.walkFilterTri(curNode, q)
		case *expr.NullNode:
			return nil, fmt.Errorf("Use != NULL instead of NOT NULL")
		default:
			u.Warnf("not implemented: %#v", node)
			u.Warnf("Unknown token %v", node.Operator.T)
			return nil, fmt.Errorf("not implemented unary function: %v", node.String())
		}
	default:
		u.Warnf("not implemented: %#v", node)
		u.Warnf("Unknown token %v", node.Operator.T)
		return nil, fmt.Errorf("not implemented unary function: %v", node.String())
	}
	return nil, nil
}

// eval() returns
//     value, isOk, isIdentity
func (m *SQLToQuanta) eval(arg expr.Node) (value.Value, bool, bool) {
	switch arg := arg.(type) {
	case *expr.NumberNode, *expr.StringNode:
		val, ok := vm.Eval(nil, arg)
		return val, ok, false
	case *expr.IdentityNode:
		if arg.IsBooleanIdentity() {
			return value.NewBoolValue(arg.Bool()), true, false
		}
		_, r, aliased := arg.LeftRight()
		f := arg.Text
		if aliased {
			f = r
		}
		table := m.conn.TableBuffers[m.tbl.Name].Table
		if _, err := table.GetAttribute(f); err != nil && f != "@timestamp" {
			return nil, false, false
		}
		return value.NewStringValue(f), true, true
	case *expr.ArrayNode:
		val, ok := vm.Eval(nil, arg)
		return val, ok, false

	}
	return nil, false, false
}


func (m *SQLToQuanta) checkFuncArgs(f *expr.FuncNode) (int, error) {

	idCount := 0
	for _, x := range f.Args {
		ids := expr.FindAllIdentities(x)
		for _, n := range ids {
			var tableName, fieldName string
			l, r, isLr := n.LeftRight()
			if isLr {
				tableName = l
				fieldName = r
			} else {
				fieldName = n.Text
				tableName = m.tbl.Name
			}
			table := m.conn.TableBuffers[tableName].Table
			_, err := table.GetAttribute(fieldName)
			if err != nil {
				return 0, fmt.Errorf("cannot resolve field %s.%s in %v() function argument", tableName, fieldName, f.Name)
			}
		}
		idCount += len(ids)
	}
	return idCount, nil
}

// Aggregations from the <select_list>
//
//    SELECT <select_list> FROM ... WHERE
//
func (m *SQLToQuanta) walkSelectList(q *shared.QueryFragment) error {

	// Do a dup check to make sure columns are aliased first.
	dupMap := make(map[string]*rel.Column, len(m.sel.Columns))
	for i := 0;  i < len(m.sel.Columns); i++ {
		c := m.sel.Columns[i]
		if c.As != "" {
			if _, found := dupMap[c.As]; found {
				return fmt.Errorf("Duplicate column %s found at position %d, needs alias", c.As, i)
			}
			dupMap[c.As] = c
			continue
		}
		if _, found := dupMap[c.SourceOriginal]; found {
			return fmt.Errorf("Duplicate column %s found at position %d, needs alias", c.SourceOriginal, i)
		}
		dupMap[c.SourceOriginal] = c
	}

	for i := len(m.sel.Columns) - 1; i >= 0; i-- {
		col := m.sel.Columns[i]
		//u.Debugf("i=%d of %d  %v %#v ", i, len(m.sel.Columns), col.Key(), col)
		if col.Expr != nil {
			switch curNode := col.Expr.(type) {
			// case *expr.NumberNode:
			//     return nil, value.NewNumberValue(curNode.Float64), nil
			// case *expr.BinaryNode:
			//     return m.walkBinary(curNode)
			// case *expr.TriNode: // Between
			//     return m.walkTri(curNode)
			// case *expr.UnaryNode:
			//     return m.walkUnary(curNode)
			case *expr.FuncNode:
   				if curNode.Missing && curNode.Name != "min" && curNode.Name != "max" && curNode.Name != "topn" {
					return fmt.Errorf("func %q not found while processing select list", curNode.Name)
				}
				count, err := m.checkFuncArgs(curNode)
				if err != nil {
					return err
				}
				if count > 0 {
					// Has identity arguments add to function aliases list
					m.funcAliases[col.As] = struct{}{}
				}
				// All Func Nodes are Aggregates?
				//esm, err := m.walkAggs(curNode)
				err = m.walkAggs(curNode, q)
				if err != nil {
					return err
				}
				/*
				   if err == nil && len(esm) > 0 {
				       m.aggs[col.As] = esm
				   } else if err != nil {
				       u.Error(err)
				       return err
				   }
				*/
				//u.Debugf("esm: %v:%v", col.As, esm)
				//u.Debugf(curNode.String())
			// case *expr.ArrayNode:
			//     return m.walkArrayNode(curNode)
			// case *expr.IdentityNode:
			//     return nil, value.NewStringValue(curNode.Text), nil
			// case *expr.StringNode:
			//     return nil, value.NewStringValue(curNode.Text), nil
			case *expr.IdentityNode:
				if col.Star || strings.HasSuffix(col.As, ".*") {
					continue
				}
				colName := curNode.String()
				if _, r, isAliased := curNode.LeftRight(); isAliased {
					colName = r
				}
				_, _, err := m.ResolveField(colName)
				if err != nil {
					return err
				}
				//u.Debugf("likely a projection, not agg T:%T  %v", curNode, curNode)
			default:
				u.Warnf("unrecognized not agg T:%T  %v", curNode, curNode)
				//panic("Unrecognized node type")
			}
		}

	}
	return nil
}

// aggregate expressions when used ast part of <select_list>
// - For Aggregates (functions) it builds appropriate underlying aggregation/map-reduce
// - For Projections (non-functions) it does nothing, that will be done later during projection
func (m *SQLToQuanta) walkAggs(cur expr.Node, q *shared.QueryFragment) error {
	switch curNode := cur.(type) {
	// case *expr.NumberNode:
	//     return nil, value.NewNumberValue(curNode.Float64), nil
	// case *expr.BinaryNode:
	//     return m.walkBinary(curNode)
	// case *expr.TriNode: // Between
	//     return m.walkTri(curNode)
	// case *expr.UnaryNode:
	//     //return m.walkUnary(curNode)
	//     u.Warnf("not implemented: %#v", curNode)
	case *expr.FuncNode:
		return m.walkAggFunc(curNode, q)
	// case *expr.ArrayNode:
	//     return m.walkArrayNode(curNode)
	// case *expr.IdentityNode:
	//     return nil, value.NewStringValue(curNode.Text), nil
	// case *expr.StringNode:
	//     return nil, value.NewStringValue(curNode.Text), nil
	default:
		u.Warnf("likely ?? not agg T:%T  %v", cur, cur)
		//panic("Unrecognized node type")
	}
	// if cur.Negate {
	// }
	return nil
}

// Take an expression func, ensure we don't do runtime-checking (as the function)
// doesn't really exist, then map that function to an Mongo Aggregation/MapReduce function
//
//    min, max, avg, sum, cardinality, terms
//
// Single Value Aggregates:
//       min, max, avg, sum, cardinality, count
//
// MultiValue aggregates:
//      terms, ??
//
func (m *SQLToQuanta) walkAggFunc(node *expr.FuncNode, q *shared.QueryFragment) error {
	switch funcName := strings.ToLower(node.Name); funcName {
	case "max", "min", "avg", "sum", "cardinality":
		m.hasSingleValue = true
		if len(node.Args) != 1 {
			//u.Debugf("not able to run as native query, running polyfill: %s", node.String())
			//return nil, fmt.Errorf("invalid func")
		}
		val, ok := eval(node.Args[0])
		if !ok {
			u.Warnf("Could not run node in backend: %v", node.String())
			m.needsPolyFill = true
		} else {
			// "min_price" : { "min" : { "field" : "price" } }
			//q = M{funcName: M{"field": val.ToString()}}
		}
		m.aggField = val.ToString()
		if funcName == "sum" {
			m.isSum = true
			m.needsPolyFill = false
		}
		if funcName == "avg" {
			m.isAvg = true
			m.needsPolyFill = false
		}
		if funcName == "min" {
			m.isMin = true
			m.needsPolyFill = false
		}
		if funcName == "max" {
			m.isMax = true
			m.needsPolyFill = false
		}
		_, bsi, err := m.ResolveField(m.aggField)
		if err != nil {
			return err
		}
		if !bsi && m.isSum {
			return fmt.Errorf("can't sum a non-bsi field %s", m.aggField)
		}
		if !bsi && m.isAvg {
			return fmt.Errorf("can't average a non-bsi field %s", m.aggField)
		}
		if !bsi && m.isMin {
			return fmt.Errorf("can't find the minimum of a non-bsi field %s", m.aggField)
		}
		if !bsi && m.isMax {
			return fmt.Errorf("can't find the maximum of a non-bsi field %s", m.aggField)
		}
		return nil
		/*
		   case "terms":
		       m.hasMultiValue = true
		       // "products" : { "terms" : {"field" : "product", "size" : 5 }}

		       if len(node.Args) == 0 || len(node.Args) > 2 {
		           return nil, fmt.Errorf("invalid terms function terms(field,10) OR terms(field)")
		       }
		       val, ok := eval(node.Args[0])
		       if !ok {
		           u.Errorf("must be valid: %v", node.String())
		       }
		       if len(node.Args) >= 2 {
		           size, ok := vm.Eval(nil, node.Args[1])
		           if !ok {
		               u.Errorf("must be valid size: %v", node.Args[1].String())
		           }
		           // "products" : { "terms" : {"field" : "product", "size" : 5 }}
		           //q = M{funcName: M{"field": val.ToString(), "size": size.Value()}}
		       } else {

		           //q = M{funcName: M{"field": val.ToString()}}
		       }
		*/

	case "topn":
		m.hasSingleValue = true
		if len(node.Args) < 1 || len(node.Args) > 2 {
			u.Errorf("must be valid: %v", node.String())
		}
		val, ok := eval(node.Args[0])
		if !ok {
			u.Warnf("Could not run node in backend: %v", node.String())
		}
		m.aggField = val.ToString()
		m.isTopn = true
		m.needsPolyFill = false
		m.topn = 0
		if len(node.Args) == 2 {
			if val, ok := eval(node.Args[1]); ok {
				if v, ok2 := val.(value.NumericValue); ok2 {
					m.topn = int(v.Int())
				}
			}
		}
		// rewrite select to include projection
		c1n := "topn_" + m.aggField
		c2n := "topn_count"
		c3n := "topn_percent"
		m.sel.Columns = []*rel.Column{rel.NewColumn(c1n), rel.NewColumn(c2n), rel.NewColumn(c3n)}

		//m.sel.From[0].BuildColIndex([]string{c1n, c2n})
		// rewrite projection to be fullfilled in reader
		c1 := rel.NewResultColumn(c1n, 0, rel.NewColumn(c1n), value.StringType)
		c2 := rel.NewResultColumn(c2n, 1, rel.NewColumn(c2n), value.IntType)
		c3 := rel.NewResultColumn(c3n, 2, rel.NewColumn(c3n), value.NumberType)
		m.p.Proj = rel.NewProjection()
		m.p.Proj.Columns = []*rel.ResultColumn{c1, c2, c3}
		m.p.Proj.Final = true
		_, bsi, err := m.ResolveField(m.aggField)
		if err != nil {
			return err
		}
		if bsi {
			return fmt.Errorf("can't rank BSI field %s", m.aggField)
		}
	case "count":
		m.hasSingleValue = true
		//u.Warnf("how do we want to use count(*)?  ?")
		val, ok := eval(node.Args[0])
		if !ok {
			u.Errorf("must be valid: %v", node.String())
			return fmt.Errorf("invalid argument: %v", node.String())
		}
		if val.ToString() == "*" {
			//return nil, nil
			//return M{"$sum": 2}, nil
		} else {
			//return M{"exists": M{"field": val.ToString()}}, nil
		}

	//default:
	//	u.Warnf("not implemented ")
	}
	u.Debugf("func:  %v", q)
	if q != nil {
		return nil
	}
	return fmt.Errorf("not implemented")
}

func eval(cur expr.Node) (value.Value, bool) {
	switch curNode := cur.(type) {
	case *expr.IdentityNode:
		if curNode.IsBooleanIdentity() {
			return value.NewBoolValue(curNode.Bool()), true
		}
		return value.NewStringValue(curNode.Text), true
	case *expr.StringNode:
		return value.NewStringValue(curNode.Text), true
	case *expr.NumberNode:
		return value.NewStringValue(curNode.Text).IntValue(), true
	default:
		//u.Errorf("unrecognized T:%T  %v", cur, cur)
	}
	return value.NilValueVal, false
}

// WalkExecSource - Implementation of WalkExecSource query processing.
func (m *SQLToQuanta) WalkExecSource(p *plan.Source) (exec.Task, error) {

	if p.Stmt == nil {
		return nil, fmt.Errorf("plan did not include Sql Statement")
	}
	if p.Stmt.Source == nil {
		return nil, fmt.Errorf("plan did not include Sql Select Statement")
	}
	if m.q.IsEmpty() {
		return nil, fmt.Errorf("query must have a predicate")
	}

	if m.limit == 0 {
		m.limit = DefaultLimit
	}

	if m.p == nil {
		//u.Debugf("custom? %v", p.Custom)
		// If we are operating in distributed mode it hasn't
		// been planned?   WE probably should allow raw data to be
		// passed via plan?
		// if _, err := m.WalkSourceSelect(nil, p); err != nil {
		//     u.Errorf("could not plan")
		//     return nil, err
		// }
		m.p = p
		if p.Custom.Bool("poly_fill") {
			m.needsPolyFill = true
		}
		/*
		   if partitionId := p.Custom.String("partition"); partitionId != "" {
		       if p.Tbl.Partition != nil {
		           for _, pt := range p.Tbl.Partition.Partitions {
		               if pt.Id == partitionId {
		                   //u.Debugf("partition: %s   %#v", partitionId, pt)
		                   m.partition = pt
		                   var partitionFilter bson.M
		                   if pt.Left == "" {
		                       partitionFilter = bson.M{p.Tbl.Partition.Keys[0]: bson.M{"$lt": pt.Right}}
		                   } else if pt.Right == "" {
		                       partitionFilter = bson.M{p.Tbl.Partition.Keys[0]: bson.M{"$gte": pt.Left}}
		                   }
		                   if len(m.filter) == 0 {
		                       m.filter = partitionFilter
		                   } else {
		                       m.filter = bson.M{"$and": []bson.M{partitionFilter, m.filter}}
		                   }
		               }
		           }
		       }
		   }
		*/
	}

	var err error
	//m.conn, err = m.s.sessionPool.Borrow(m.q.GetRootIndex())
	m.conn, err = m.s.sessionPool.Borrow(m.tbl.Name)
	if err != nil {
		return nil, fmt.Errorf("Error opening Quanta session %v", err)
	}
	//defer m.s.sessionPool.Return(m.q.GetRootIndex(), m.conn)
	defer m.s.sessionPool.Return(m.tbl.Name, m.conn)
	ctx := p.Context()
	//hasJoin := len(p.Stmt.Source.From) > 0
	//u.Infof("Projection:  %T:%p   %T:%p", proj, proj, proj.Proj, proj.Proj)
	hasAliasedStar := len(ctx.Projection.Proj.Columns) == 1 &&
		strings.HasSuffix(ctx.Projection.Proj.Columns[0].As, ".*")
	if p.Stmt.Source.Star || hasAliasedStar {
		ctx.Projection.Proj, _, _, _, _, err = createProjection(p.Stmt.Source, p.Schema, "", nil)
		if err != nil {
			return nil, err
		}
	}
	orig := ctx.Stmt.(*rel.SqlSelect)
	if orig.IsAggQuery() {
		ctx.Projection.Proj = rel.NewProjection()
		ctx.Projection.Proj.Final = true
		nm := orig.Columns[0].As
		c1 := rel.NewResultColumn(nm, 0, rel.NewColumn(nm), value.IntType)
		ctx.Projection.Proj.Columns = []*rel.ResultColumn{c1}
	}
	m.TaskBase = exec.NewTaskBase(ctx)
	m.sel = p.Stmt.Source
	//u.Debugf("sqltopql plan sql?  %#v", p.Stmt)
	//u.Debugf("sqltopql plan sql.Source %#v", p.Stmt.Source)

	//filterBy, _ := json.Marshal(m.filter)
	//u.Infof("tbl %#v", m.tbl.Columns(), m.tbl)
	//u.Infof("filter: %#v  \n%s", m.filter, filterBy)
	//u.Debugf("db=%v  tbl=%v filter=%v sort=%v limit=%v skip=%v", m.schema.Name, m.tbl.Name, string(filterBy), m.sort, m.sel.Limit, m.sel.Offset)

	var response *shared.BitmapQueryResponse

	// handle "LIKE"
	err = m.q.Visit(func(f *shared.QueryFragment) error {
		// For like operator invoke search client and pass resulting hashcode list as BATCH_EQ
		if f.Operation == "LIKE_UNION" || f.Operation == "LIKE_INTERSECT" {
			start := time.Now()
			results, err := m.conn.StringIndex.Search(f.Search)
			if err != nil {
				return err
			}
			u.Infof("LIKE '%s' matches %d items.\n", f.Search, len(results))
			elapsed := time.Since(start)
			u.Infof("Text search done in %v. Passing hashcodes to query.\n", elapsed)
			values := make([]int64, len(results))
			j := 0
			for result := range results {
				values[j] = int64(result)
				j++
			}
			if f.Operation == "LIKE_UNION" {
				f.Operation = "UNION"
			} else {
				f.Operation = "INTERSECT"
			}
			f.BSIOp = "BATCH_EQ"
			f.Values = values
		}
		return nil
	})

	if err != nil {
		u.Errorf("%v", err)
		return nil, err
	}

	m.q.Dump()
	start := time.Now()
	response, err = m.conn.BitIndex.Query(m.q)
	elapsed := time.Since(start)
	u.Debugf("Elapsed time %s\n", elapsed)
	u.Debugf("SQL = %v\n", m.sel)

	if err != nil {
		u.Errorf("%v", err)
	}
	//query := m.sess.DB(m.schema.Name).C(m.tbl.Name).Find(m.filter)
	// if len(m.sort) > 0 {
	//     query = query.Sort(m.sort)
	// }

	// Where clause will be processed in the source, so replace where clause with a filter that resolves to true
	//dummyWhere, _ := expr.ParseExpression("1=1")
	//m.sel.Where = rel.NewSqlWhere(dummyWhere)

	//u.LogTraceDf(u.WARN, 16, "hello")
	resultReader := NewResultReader(m.conn, m, response, m.limit, m.offset)
	m.resp = resultReader

	//u.Debugf("sqltopql: %p  resultreader: %p sourceplan: %p argsource:%p ", m, m.resp, m.p, p)
	/*
	   if len(m.sel.OrderBy) > 0 {
	       sorts := make([]string, len(m.sel.OrderBy))
	       for i, col := range m.sel.OrderBy {
	           // TODO: look at any funcs?   walk these expressions?
	           switch col.Order {
	           case "ASC":
	               sorts[i] = col.As
	           case "DESC":
	               sorts[i] = fmt.Sprintf("-%s", col.As)
	           default:
	               // default sorder order = "naturalorder"
	               sorts[i] = col.As
	           }
	       }
	       //query = query.Sort(sorts...)
	       op := plan.NewOrder(m.sel)
	       ot := exec.NewOrder(ctx, op)
	       resultReader.Add(ot)
	       m.needsPolyFill = true
	   }
	*/

	return resultReader, err
}

// CreateMutator part of Mutator interface to allow data sources create a stateful
//  mutation context for update/delete operations.
func (m *SQLToQuanta) CreateMutator(pc interface{}) (schema.ConnMutator, error) {
	if ctx, ok := pc.(*plan.Context); ok && ctx != nil {
		m.TaskBase = exec.NewTaskBase(ctx)
		m.stmt = ctx.Stmt
		return m, nil
	}
	return nil, fmt.Errorf("expected *plan.Context but got %T", pc)
}

// PatchWhere - Handle SQL updates
func (m *SQLToQuanta) PatchWhere(ctx context.Context, where expr.Node, patch interface{}) (int64, error) {

	m.q = shared.NewBitmapQuery()
	frag := m.q.NewQueryFragment()
	m.startDate = ""
	m.endDate = ""

	var err error
	m.conn, err = m.s.sessionPool.Borrow(m.tbl.Name)
	if err != nil {
		return 0, fmt.Errorf("Error opening Quanta session %v", err)
	}
	defer m.s.sessionPool.Return(m.tbl.Name, m.conn)

	if where != nil {
		_, err = m.walkNode(where, frag)
		if err != nil {
			u.Warnf("Could Not evaluate Where Node %s %v", where.String(), err)
			return 0, err
		}
	}

	if m.q.IsEmpty() {
		return 0, fmt.Errorf("update statement must have a predicate")
	}

	if m.startDate == "" {
		m.startDate = "1970-01-01T00"
	}
	if m.endDate == "" {
		end := time.Now().AddDate(0, 0, 1)
		m.endDate = end.Format(shared.YMDHTimeFmt)
	}

	m.q.FromTime = m.startDate
	m.q.ToTime = m.endDate

	response, err := m.conn.BitIndex.Query(m.q)
	if err != nil {
		return 0, fmt.Errorf("Update query failed - %v", err)
	}

	results := response.Results.ToArray()
	if len(results) != 1 {
		return 0, fmt.Errorf("expecting 1 result from update query but got %d", response.Count)
	}

	valueMap := make(map[string]*rel.ValueColumn)
	for k, v := range patch.(map[string]driver.Value) {
		valueMap[k] = &rel.ValueColumn{Value: value.NewValue(v)}
	}

	var timeFmt = shared.YMDHTimeFmt
	updColID := results[0]
	partition := time.Unix(0, int64(updColID))
	table := m.conn.TableBuffers[m.tbl.Name].Table
	if table.TimeQuantumType == "YMD" {
		timeFmt = shared.YMDTimeFmt
	}
	partStr := partition.Format(timeFmt)
	partition, _ = time.Parse(timeFmt, partStr)

	return m.updateRow(m.tbl.Name, updColID, valueMap, partition)
}

// Put Interface for inserts.  Updates are handled by PatchWhere
func (m *SQLToQuanta) Put(ctx context.Context, key schema.Key, val interface{}) (schema.Key, error) {

	if m.schema == nil {
		u.Warnf("must have schema")
		return nil, fmt.Errorf("must have schema for update/insert")
	}

	cols := m.tbl.Columns()
	if m.stmt == nil {
		return nil, fmt.Errorf("must have stmts to infer columns ")
	}

	conn, err := m.s.sessionPool.Borrow(m.tbl.Name)
	if err != nil {
		return nil, fmt.Errorf("Error opening Quanta session %v", err)
	}
	defer m.s.sessionPool.Return(m.tbl.Name, conn)
	m.conn = conn
	//u.Infof("STMT = %v, VALS = %v\n", m.stmt, val)
	//u.Infof("INITIAL COLS = %v\n", cols)

	switch q := m.stmt.(type) {
	case *rel.SqlInsert:
		cols = q.ColumnNames()
	case *rel.SqlUpdate:
		return nil, fmt.Errorf("should not be here - Update happen via PatchWhere")
	default:
		return nil, fmt.Errorf("%T not yet supported ", q)
	}

	// Everything from here on is an INSERT
	var row []driver.Value
	var vMap map[string]interface{} = make(map[string]interface{})
	var colID uint64 = 0
	colNames := make(map[string]int, len(m.tbl.Fields))

	for i, f := range m.tbl.Fields {
		colNames[f.Name] = i
	}
	curRow := make([]interface{}, len(m.tbl.Fields))
	tbuf := m.conn.TableBuffers[m.tbl.Name]
	tbuf.CurrentColumnID = 0
	table := tbuf.Table

	switch valT := val.(type) {
	case []driver.Value:
		row = valT
		//u.Infof("row:  %v", row)
		//u.Infof("row len=%v   fieldlen=%v col len=%v", len(row), len(m.tbl.Fields), len(cols))
		for j, f := range m.tbl.Fields {
			found := false // column found in list of insert values
			for i, colName := range cols {
				if f.Name == colName {
					found = true
					idx := colNames[colName]
					if len(row) <= i-1 {
						u.Errorf("bad column count?  %d vs %d  col: %+v", len(row), i, f)
					} else {
						switch val := row[i].(type) {
						case string:
							if val != "" {
								curRow[idx] = val
								//vMap[colName] = val
								vMap[f.Collation] = val
							}
						case []byte, int, int64, bool, time.Time, float64:
							curRow[idx] = val
							//vMap[colName] = val
							vMap[f.Collation] = val
						case []value.Value:
							switch f.ValueType() {
							case value.StringsType:
								vals := make([]string, len(val))
								for si, sv := range val {
									vals[si] = sv.ToString()
								}
								curRow[idx] = vals
							default:
								u.Warnf("what type? %v", f.Type)
								/*
								   by, err := json.Marshal(val)
								   if err != nil {
								       u.Errorf("error converting field %v  err=%v", val, err)
								       curRow[idx] = ""
								   } else {
								       curRow[idx] = string(by)
								   }
								   u.Debugf("PUT field: i=%d col=%s row[i]=%v  T:%T", i, colName, string(by), by)
								*/
							}

						default:
							u.Warnf("unsupported conversion: %T  %v", val, val)
						}
					}
					break
				}
			}
			if a, err := table.GetAttribute(f.Name); err == nil {
				if !found {
					// Check and populate default values/expr
					if a.Required && a.DefaultValue == "" {
						return nil, fmt.Errorf("value not provided for required column %s", f.Name)
					}
					// The actual default value will be populated inside PutRow()
				} else {
					// If attribute contains columnID then we won't have to allocate one.
					if a.ColumnID {
						switch val := curRow[j].(type) {
						case uint64:
							colID = curRow[j].(uint64)
						case int64:
							colID = uint64(curRow[j].(int64))
						case int:
							colID = uint64(curRow[j].(int))
						default:
							return nil, fmt.Errorf("column %s contains invalid type %T for columnID",
								f.Name, val)
						}
					}
				}
			} else {
				return nil, fmt.Errorf("cannot locate column %s", f.Name)
			}
		}

	default:
		u.Warnf("unsupported type: %T  %#v", val, val)
		return nil, fmt.Errorf("was not []driver.Value:  %T", val)
	}

	// Begin critical section
	err = m.conn.PutRow(table.Name, vMap, colID, false)
	if err != nil {
		return nil, err
	}

	newKey := datasource.NewKeyCol("id", "fixme")

	// End critical section
	return newKey, nil
}

// Call Client.Update - TODO, This fuctionality should be merged with PutRow()
func (m *SQLToQuanta) updateRow(table string, columnID uint64, updValueMap map[string]*rel.ValueColumn,
	timePartition time.Time) (int64, error) {

	tbuf, ok := m.conn.TableBuffers[table]
	if !ok {
		return 0, fmt.Errorf("table %s is not open for this session", table)
	}
	for k, vc := range updValueMap {
		a, err := tbuf.Table.GetAttribute(k)
		if err != nil {
			return 0, fmt.Errorf("attribute %s.%s is not defined", table, k)
		}
		rowID, err := a.MapValue(vc.Value.Value(), nil)
		if err != nil {
			return 0, err
		}
		err = m.conn.BitIndex.Update(table, a.FieldName, columnID, int64(rowID), timePartition, a.IsBSI(), a.Exclusive)
		if err != nil {
			return 0, err
		}
	}
	return 1, nil
}

// PutMulti - Multiple put operation handler.
func (m *SQLToQuanta) PutMulti(ctx context.Context, keys []schema.Key, src interface{}) ([]schema.Key, error) {
	return nil, schema.ErrNotImplemented
}

// Delete by row
func (m *SQLToQuanta) Delete(key driver.Value) (int, error) {
	u.Debugf("hm, in delete?  %v", key)
	return 0, schema.ErrNotImplemented
}

// DeleteExpression - delete by expression (where clause)
//  - For where columns we can query
//  - for others we might have to do a select -> delete
func (m *SQLToQuanta) DeleteExpression(p interface{}, where expr.Node) (int, error) {

	u.Debugf("In delete?  %v   %T", where, p)
	pd, ok := p.(*plan.Delete)
	if !ok {
		return 0, plan.ErrNoPlan
	}
	_ = pd

	// Construct query
	m.q = shared.NewBitmapQuery()
	frag := m.q.NewQueryFragment()

	var err error
	m.conn, err = m.s.sessionPool.Borrow(m.tbl.Name)
	if err != nil {
		return 0, fmt.Errorf("Error opening Quanta session %v", err)
	}
	defer m.s.sessionPool.Return(m.tbl.Name, m.conn)

	if where != nil {
		_, err = m.walkNode(where, frag)
		if err != nil {
			//u.Warnf("Could Not evaluate Where Node %s %v", req.Where.Expr.String(), err)
			u.Warnf("Could Not evaluate Where Node %s %v", where.String(), err)
			return 0, err
		}
	}

	if m.q.IsEmpty() {
		return 0, fmt.Errorf("query must have a predicate")
	}

	m.q.FromTime = m.startDate
	m.q.ToTime = m.endDate

	m.q.Dump()
	var response *shared.BitmapQueryResponse
	response, err = m.conn.BitIndex.Query(m.q)
	if err != nil {
		return 0, err
	}

	err = m.conn.BitIndex.BulkClear(m.q.GetRootIndex(), m.q.FromTime, m.q.ToTime, response.Results)
	if err != nil {
		return 0, err
	}
	m.conn.Flush()

	return int(response.Results.GetCardinality()), nil
}

// SchemaInfoString implements schemaInfo Key()
type SchemaInfoString struct {
	k string
}

// Key returns col key.
func (m SchemaInfoString) Key() string { return m.k }
