package core

// Projection functions including join projection handling.

import (
	"database/sql/driver"
	"fmt"
	u "github.com/araddon/gou"
	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/disney/quanta/shared"
	"math"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	timeFmt = "2006-01-02T15"
)

// Projector - State of an in-flight projection
type Projector struct {
	connection     *Session
	fromTime       int64
	toTime         int64
	projAttributes []*Attribute
	joinAttributes []*Attribute
	projFieldMap   map[string]int // Fields to be output in the projection
	foundSets      map[string]*roaring64.Bitmap
	driverTable    string
	joinTypes      map[string]bool // nil for non-join projections
	resultIterator roaring64.ManyIntIterable64
	stateGuard     sync.Mutex
	fkBSI          map[string]*roaring64.BSI
	Prefetch       bool                                      // Set to true to prefetch all bitmap related data for export.
	bsiResults     map[string]map[string]*roaring64.BSI      // Prefetched BSIs
	bitmapResults  map[string]map[string]*BitmapFieldResults // Prefetched Bitmaps
	negate         bool                                      // != join
}

// BitmapFieldResults - All RowID values for a bitmap field/attribute sorted by cardinality descending
type BitmapFieldResults struct {
	fieldRows []*BitmapFieldRow
}

// BitmapFieldRow - A result row for "standard" bitmaps
type BitmapFieldRow struct {
	rowID uint64
	bm    *roaring64.Bitmap
}

// NewProjection - Construct a Projection.
func NewProjection(s *Session, foundSets map[string]*roaring64.Bitmap, joinNames, projNames []string,
	driver string, fromTime, toTime int64, joinTypes map[string]bool, negate bool) (*Projector, error) {

	projFieldMap := make(map[string]int)
	j := 0
	for _, v := range projNames {
		if _, found := projFieldMap[v]; !found {
			projFieldMap[v] = j
			j++
		}
	}

	/*
	 * joinNames contains an optional list of fields that must present to resolve foreign key
	 * relationships but are not included in the output projection.
	 */
	fieldNames := make([]string, len(projFieldMap))
	for k, i := range projFieldMap {
		fieldNames[i] = k
	}
	projAttributes, err := getAttributes(s, fieldNames)
	if err != nil {
		return nil, err
	}

	p := &Projector{connection: s, projAttributes: projAttributes, joinTypes: joinTypes,
		foundSets: foundSets, fromTime: fromTime, toTime: toTime, driverTable: driver, negate: negate}

	// Perform validation for join projections (if applicable)
	if driver != "" && len(foundSets) > 1 {
		var errx error
		p.joinAttributes, errx = getAttributes(s, joinNames)
		if errx != nil {
			return nil, errx
		}
		for _, v := range p.joinAttributes {
			if v.Parent.Name != driver {
				// Make sure foreign key is in join attributes
				if _, ok := p.findRelationLink(v.Parent.Name); !ok {
					return nil, fmt.Errorf("foreign key for %s missing from projection", v.Parent.Name)
				}
			}
		}
	} else if driver == "" && len(foundSets) > 1 {
		return nil, fmt.Errorf("driver table not specified for join projection")
	} else if driver != "" {
		return nil, fmt.Errorf("driver table %s specified but only 1 foundSet was provided", driver)
	}
	if p.driverTable == "" {
		for k := range p.foundSets {
			p.driverTable = k
		}
	}
	if p.driverTable != "" && len(foundSets) > 1 {
		// retrieve relation BSI(s)
		rs := make(map[string]*roaring64.Bitmap)
		rs[p.driverTable] = p.foundSets[p.driverTable]
		u.Debugf("GET DRIVER = %v, BSI = %d", p.driverTable, rs[p.driverTable].GetCardinality())
		bsir, _, err := p.retrieveBitmapResults(rs, p.joinAttributes, false)
		if err != nil {
			return nil, err
		}
		p.fkBSI = bsir[p.driverTable]
	}
	p.projFieldMap = projFieldMap

	if p.joinTypes == nil {
		p.joinTypes = make(map[string]bool)
	}
	// If a joinType is missing then assume inner.  The value is true for inner joins, false for outer.
	innerJoin := true
	if jt, found := p.joinTypes[p.driverTable]; found {
		innerJoin = jt
	}
	u.Debugf("INNER JOIN = %v", innerJoin)

	driverSet := p.foundSets[p.driverTable]
	// For inner joins filter out any rows in the driver table not in fkBSI link
	if innerJoin && !negate {
		for _, v := range p.fkBSI {
			driverSet.And(v.GetExistenceBitmap())
		}
	}
	// filter out entries from driver found set not contained within FKBSIs
	for k, v := range p.foundSets {
		if !innerJoin {
			continue
		}
		if k == p.driverTable {
			continue
		}
		// If it is an anti-join (negate) then retrieve the primary key BSI.
		fka, ok := p.findRelationLink(k)
		if !ok {
			return nil, fmt.Errorf("NewProjection: Cannot resolve FK relationship for %s", k)
		}
		fkBsi, ok2 := p.fkBSI[fka.FieldName]
		if !ok2 {
			return nil, fmt.Errorf("NewProjection: FK BSI lookup failed for %s - %s",
				p.driverTable, fka.FieldName)
		}

		u.Debugf("FKBSI  %v = %d", k, fkBsi.GetCardinality())
		newSet := fkBsi.Transpose()
		filterSet := v.Clone()
		// Anti-join
		if negate {
			filterSet.AndNot(newSet)
		} else {
			filterSet.And(newSet)
		}
		p.foundSets[k] = filterSet
	}

	p.resultIterator = driverSet.ManyIterator()

	return p, nil
}

func getAttributes(s *Session, fieldNames []string) ([]*Attribute, error) {

	attributes := make([]*Attribute, len(fieldNames))

	// build up projection metadata
	for i, v := range fieldNames {
		st := strings.Split(v, ".")
		if len(st) != 2 {
			return nil, fmt.Errorf("field names must be in the form <tableName>.<attributeName> [%v]", v)
		}
		tableName := st[0]
		attributeName := st[1]
		if attributeName == "" {
			return nil, fmt.Errorf("attribute name missing for [%s]", v)
		}
		tbuf, ok := s.TableBuffers[tableName]
		if !ok {
			return nil, fmt.Errorf("table %s invalid or not opened", tableName)
		}
		a, err := tbuf.Table.GetAttribute(attributeName)
		if err != nil {
			return nil, fmt.Errorf("attribute %s invalid - %s.%s", attributeName, st[0], st[1])
		}
		attributes[i] = a
	}
	return attributes, nil
}

// retrieveBitmapResults - Populate internal structures with projection data.
func (p *Projector) retrieveBitmapResults(foundSets map[string]*roaring64.Bitmap, attr []*Attribute, negate bool) (
	map[string]map[string]*roaring64.BSI, map[string]map[string]*BitmapFieldResults, error) {

	fieldNames := make(map[string][]string)
	for _, v := range attr {
		if _, ok := fieldNames[v.Parent.Name]; !ok {
			fieldNames[v.Parent.Name] = make([]string, 0)
		}
		fieldNames[v.Parent.Name] = append(fieldNames[v.Parent.Name], v.FieldName)
	}

	bsiResults := make(map[string]map[string]*roaring64.BSI)
	bitmapResults := make(map[string]map[string]*BitmapFieldResults)

	for k, v := range foundSets {
		if len(fieldNames[k]) == 0 {
			continue
		}
		u.Debugf("TABLE = %v, FIELDNAMES = %#v, FS = %d, NEGATE = %v", k, fieldNames[k], v.GetCardinality(), negate)
		bsir, bitr, err := p.connection.BitIndex.Projection(k, fieldNames[k], p.fromTime, p.toTime, v, false)
		if err != nil {
			return nil, nil, err
		}
		for field, r := range bitr {
			if _, ok := bitmapResults[k]; !ok {
				bitmapResults[k] = make(map[string]*BitmapFieldResults)
			}
			bitmapResults[k][field] = NewBitmapFieldResults(r)
		}
		bsiResults[k] = bsir
	}

	return bsiResults, bitmapResults, nil
}

// findRelation - Retrieves the foreign key BSI field to be used for join projections
func (p *Projector) findRelationLink(tableName string) (*Attribute, bool) {
	attr := append(p.joinAttributes, p.projAttributes...)
	for _, v := range attr {
		if v.ForeignKey != "" {
			fkTable, _, err := v.GetFKSpec()
			if err != nil {
				return nil, false
			}
			if fkTable == tableName {
				return v, true
			}
		}
	}
	return nil, false
}

func (p *Projector) nextSets(columnIDs []uint64) (map[string]map[string]*roaring64.BSI,
		map[string]map[string]*BitmapFieldResults, error) {

	bsiResults := make(map[string]map[string]*roaring64.BSI)
	bitmapResults := make(map[string]map[string]*BitmapFieldResults)

	rs := make(map[string]*roaring64.Bitmap)
	driverSet := roaring64.BitmapOf(columnIDs...)
	rs[p.driverTable] = driverSet
	bsir, bitr, err := p.retrieveBitmapResults(rs, p.projAttributes, false)
	if err != nil {
		return nil, nil, err
	}
	bsiResults[p.driverTable] = bsir[p.driverTable]
	bitmapResults[p.driverTable] = bitr[p.driverTable]

	for k, v := range p.foundSets {
		if k == p.driverTable {
			continue
		}
		fka, ok := p.findRelationLink(k)
		if !ok {
			return nil, nil, fmt.Errorf("cannot resolve FK relationship for %s", k)
		}
		fkBsi, ok2 := bsiResults[p.driverTable][fka.FieldName]
		if !ok2 {
			//return nil, nil, fmt.Errorf("FK BSI lookup failed for %s - %s", p.driverTable, fka.FieldName)
			continue
		}
		newSet := fkBsi.Transpose()
		rs = make(map[string]*roaring64.Bitmap)
		innerJoin := true
		if jt, found := p.joinTypes[k]; found {
			innerJoin = jt
		}
		if innerJoin {
			newSet.And(v)
		}
		rs[k] = newSet
		//bsir, bitr, err := p.retrieveBitmapResults(rs, p.projAttributes, p.negate)
		bsir, bitr, err := p.retrieveBitmapResults(rs, p.projAttributes, false)
		if err != nil {
			return nil, nil, err
		}
		bsiResults[k] = bsir[k]
		bitmapResults[k] = bitr[k]
	}
	return bsiResults, bitmapResults, nil
}

// Next - Return next projection batch.  This can be called by multiple threads in parallel for maximum throughput.
func (p *Projector) Next(count int) (columnIDs []uint64, rows [][]driver.Value, err error) {

	columnIDs = make([]uint64, count)
	rows = make([][]driver.Value, 0)
	p.stateGuard.Lock()
	actualCount := p.resultIterator.NextMany(columnIDs)
	p.stateGuard.Unlock()
	if actualCount < count {
		columnIDs = append([]uint64(nil), columnIDs[:actualCount]...) // resize
	}
	if actualCount == 0 {
		return
	}

	var bsir map[string]map[string]*roaring64.BSI
	var bitr map[string]map[string]*BitmapFieldResults
	if p.Prefetch {
		p.stateGuard.Lock()
		if p.bsiResults == nil && p.bitmapResults == nil {
			// No cached results and Prefetch is true so populate cache
			allAttr := make([]*Attribute, 0)
			allAttr = append(allAttr, p.projAttributes...)
			allAttr = append(allAttr, p.joinAttributes...)
			bsiResults, bitmapResults, e := p.retrieveBitmapResults(p.foundSets, allAttr, false)
			if e != nil {
				p.stateGuard.Unlock()
				err = e
				return
			}

			p.bsiResults = bsiResults
			p.bitmapResults = bitmapResults
		}
		bsir = p.bsiResults
		bitr = p.bitmapResults
		p.stateGuard.Unlock()
	} else {
		var e error
		bsir, bitr, e = p.nextSets(columnIDs)
		if e != nil {
			err = e
			return
		}
	}

	// Perform forward fetch of strings
	strMap, e := p.fetchStrings(columnIDs, bsir)
	if e != nil {
		err = e
		return
	}

	for _, v := range columnIDs {
		row, e := p.getRow(v, strMap, bsir, bitr)
		if e != nil {
			err = e
			return
		}
		rows = append(rows, row)
	}
	return
}

func (p *Projector) fetchStrings(columnIDs []uint64, bsiResults map[string]map[string]*roaring64.BSI) (
	map[string]map[interface{}]interface{}, error) {

	strMap := make(map[string]map[interface{}]interface{})
	var trxColumnIDs []uint64
	for _, v := range p.projAttributes {
		if v.MappingStrategy != "StringHashBSI" && v.MappingStrategy != "ParentRelation" {
			continue
		}
		lookupAttribute := v
		/*
		 * In a nested structure, the relation link field often doesn't have a source
		 * including the relation link in a projection will resolve the backing data
		 * without requiring an explicit join.
		 */
		if v.MappingStrategy == "ParentRelation" || v.Parent.Name != p.driverTable {
			relation := v.Parent.Name
			if v.MappingStrategy == "ParentRelation" {
				var errx error
				relation, _, errx = v.GetFKSpec()
				if errx != nil {
					return nil, fmt.Errorf("Projector error - GetFKSpec() - [%v]", errx)
				}
			}
			linkAttr, ok := p.findRelationLink(relation)
			if !ok {
				return nil, fmt.Errorf("Projector error: could not find relation link for %s", relation)
			}
			key := linkAttr.FieldName
			if relBuf, ok := p.connection.TableBuffers[relation]; ok {
				// use FK IntBSI to transpose to parent columnID set
				//if _, ok := bsiResults[p.driverTable][key]; !ok {
				if _, ok := bsiResults[p.driverTable][key]; !ok {
					continue
					//return nil, fmt.Errorf("no BSI results for %s - %s", p.driverTable, key)
				}
				trxColumnIDs = p.transposeFKColumnIDs(bsiResults[p.driverTable][key], columnIDs)
				if v.MappingStrategy == "ParentRelation" {
					if strings.HasSuffix(v.ForeignKey, "@rownum") {
						continue
					}
					if len(relBuf.PKAttributes) > 1 {
						return nil, fmt.Errorf("Projector error - Can only support single PK with link [%s]", key)
					}
					pv := relBuf.PKAttributes[0]
					if pv.MappingStrategy != "StringHashBSI" {
						continue
					}
					lookupAttribute = pv
				}
			}
		}
		var lBatch map[interface{}]interface{}
		var err error
		if lookupAttribute.Parent.Name != p.driverTable {
			lBatch, err = p.getPartitionedStrings(lookupAttribute, trxColumnIDs)
		} else {
			lBatch, err = p.getPartitionedStrings(lookupAttribute, columnIDs)
		}
		if err != nil {
			return nil, err
		}
		strMap[v.FieldName] = lBatch
	}

	return strMap, nil
}

func (p *Projector) transposeFKColumnIDs(fkBSI *roaring64.BSI, columnIDs []uint64) (newColumnIDs []uint64) {

	foundSet := roaring64.BitmapOf(columnIDs...)
	newSet := fkBSI.IntersectAndTranspose(0, foundSet)
	newColumnIDs = newSet.ToArray()
	return
}


func (p *Projector) getRow(colID uint64, strMap map[string]map[interface{}]interface{},
	bsiResults map[string]map[string]*roaring64.BSI,
	bitmapResults map[string]map[string]*BitmapFieldResults) (row []driver.Value, err error) {

	row = make([]driver.Value, len(p.projFieldMap))
	for _, v := range p.projAttributes {
		i, projectable := p.projFieldMap[fmt.Sprintf("%s.%s", v.Parent.Name, v.FieldName)]
		if !projectable {
			continue
		}
		innerJoin := true
		if jt, found := p.joinTypes[v.Parent.Name]; found {
			innerJoin = jt
		}
		if v.MappingStrategy == "StringHashBSI" {
			cid, err2 := p.checkColumnID(v, colID, bsiResults)
			if err2 != nil {
				if !innerJoin {
					row[i] = "NULL"
					continue
				}
				err = err2
				return
			}
			if str, ok := strMap[v.FieldName][cid]; ok {
				if str == "" {
					row[i] = "NULL"
				} else {
					row[i] = str
				}
			} else {
				row[i] = "NULL"
			}
			continue
		}
		if v.MappingStrategy == "ParentRelation" {
			relation, _, errx := v.GetFKSpec()
			if errx != nil {
				err = fmt.Errorf("Projector error - getRow() - [%v]", errx)
				return
			}
			if relBuf, ok := p.connection.TableBuffers[relation]; ok {
				if len(relBuf.PKAttributes) > 1 && !strings.HasSuffix(v.ForeignKey, "@rownum") {
					err = fmt.Errorf("Projector error - Can only support single PK with link [%s]", v.FieldName)
					return
				}
				pv := relBuf.PKAttributes[0]
				if pv.MappingStrategy == "StringHashBSI" {
					bsi, found := bsiResults[v.Parent.Name][v.FieldName]
					if !found {
						row[i] = "NULL"
						continue
					}
					if val, ok := bsi.GetValue(colID); ok {
						if str, ok := strMap[v.FieldName][uint64(val)]; ok {
							row[i] = str
						} else {
							row[i] = "NULL"
						}
					} else {
						row[i] = "NULL"
					}
					continue
				}
			} else {
				return nil, fmt.Errorf("foreign key %s points to table that is not open", v.FieldName)
			}
		}
		if v.IsBSI() {
			rs, eok := bsiResults[v.Parent.Name][v.FieldName]
			if !eok {
				row[i] = "NULL"
				continue
			}
			cid, err2 := p.checkColumnID(v, colID, bsiResults)
			if err2 != nil {
				if !innerJoin {
					row[i] = "NULL"
					continue
				}
				err = err2
				return
			}
			if val, ok := rs.GetValue(cid); !ok {
				row[i] = "NULL"
			} else {
				switch shared.TypeFromString(v.Type) {
				case shared.Integer:
					row[i] = fmt.Sprintf("%10d", val)
				case shared.Float:
					f := fmt.Sprintf("%%10.%df", v.Scale)
					row[i] = fmt.Sprintf(f, float64(val)/math.Pow10(v.Scale))
				case shared.Date, shared.DateTime:
					t := time.Unix(0, val*1000000).UTC()
					if v.MappingStrategy == "SysMicroBSI" {
						t = time.Unix(0, val*1000).UTC()
					}
					if shared.TypeFromString(v.Type) == shared.Date {
						row[i] = t.Format("2006-01-02")
					} else {
						switch v.MappingStrategy {
						case "SysSecBSI":
							row[i] = t.Format(time.RFC3339)
						case "SysMillisBSI":
							row[i] = t.Format("2006-01-02T15:04:05.000Z")
						default:
							row[i] = t.Format(time.RFC3339Nano)
						}
					}
				default:
					row[i] = val
				}
			}
			continue
		}

		// Must be a standard bitmap
		bmr, fok := bitmapResults[v.Parent.Name][v.FieldName]
		if !fok {
			row[i] = "NULL"
			continue
		}

		cid, err2 := p.checkColumnID(v, colID, bsiResults)
		if err2 != nil {
			if !innerJoin {
				row[i] = "NULL"
				continue
			}
			err = err2
			return
		}
		rowIDs, ok := bmr.getRowIDsForColumnID(cid)
		if !ok {
			row[i] = "NULL"
			continue
		}
		if row[i], err = v.ToBackingValue(rowIDs, p.connection); err != nil {
			return
		}
	}
	return
}

// If field is in a join table then transpose the column ID
func (p *Projector) checkColumnID(v *Attribute, cID uint64,
	bsiResults map[string]map[string]*roaring64.BSI) (colID uint64, err error) {

	if p.driverTable != "" && v.Parent.Name != p.driverTable {
		if r, ok := p.findRelationLink(v.Parent.Name); !ok {
			err = fmt.Errorf("findRelationLink failed for %s", v.Parent.Name)
		} else {
			// Translate ColID
			if b, fok := bsiResults[r.Parent.Name][r.FieldName]; !fok {
				err = fmt.Errorf("bsi lookup failed for %s - %s", r.Parent.Name, r.FieldName)
			} else {
				val, _ := b.GetValue(cID)
				colID = uint64(val)
			}
		}
	} else {
		colID = cID
	}
	return
}

func (r *BitmapFieldResults) getRowIDsForColumnID(colID uint64) ([]uint64, bool) {

	if r == nil {
		return nil, false
	}
	rowIDs := make([]uint64, 0)
	for _, v := range r.fieldRows {
		if v.bm.Contains(colID) {
			rowIDs = append(rowIDs, v.rowID)
		}
	}
	if len(rowIDs) > 0 {
		return rowIDs, true
	}
	return nil, false
}

// NewBitmapFieldResults - Construct NewBitmapFieldResults
func NewBitmapFieldResults(input map[uint64]*roaring64.Bitmap) *BitmapFieldResults {

	fieldRows := make([]*BitmapFieldRow, len(input))
	i := 0
	for k, v := range input {
		fieldRows[i] = &BitmapFieldRow{rowID: k, bm: v}
		i++
	}
	fr := &BitmapFieldResults{fieldRows: fieldRows}
	sort.Sort(fr)
	return fr
}

func (r *BitmapFieldResults) Len() int {
	return len(r.fieldRows)
}

func (r *BitmapFieldResults) Less(i, j int) bool {
	// We want rows in descending order by cardinality
	return r.fieldRows[i].bm.GetCardinality() > r.fieldRows[j].bm.GetCardinality()
}

func (r *BitmapFieldResults) Swap(i, j int) {
	r.fieldRows[i], r.fieldRows[j] = r.fieldRows[j], r.fieldRows[i]
}

// Rank - TopN rank aggregate
func (p *Projector) Rank(table, field string, count int) (rows [][]driver.Value, err error) {

	bsiResults, bitmapResults, err := p.retrieveBitmapResults(p.foundSets, p.projAttributes, false)
	if err != nil {
		return nil, err
	}

	r, ok := bitmapResults[table][field]
	if !ok {
		if _, ok2 := bsiResults[table][field]; ok2 {
			return nil, fmt.Errorf("Cannot rank BSI field '%s'", field)
		}
		return nil, fmt.Errorf("Cannot locate results for field '%s'", field)
	}
	var attr *Attribute
	for _, v := range p.projAttributes {
		if v.FieldName == field {
			attr = v
			break
		}
	}
	if attr == nil { // Should never be nil
		return nil, fmt.Errorf("Cannot locate attribute for field '%s'", field)
	}
	if count == 0 {
		count = len(r.fieldRows)
	}
	var total uint64
	var other uint64
	rows = make([][]driver.Value, 0)
	counter := 1
	for _, br := range r.fieldRows {
		row := make([]driver.Value, 3)
		row[0], err = attr.MapValueReverse(br.rowID, p.connection)
		if err != nil {
			return nil, fmt.Errorf("MapValueReverse error for field '%s' - %v", field, err)
		}
		x := br.bm.GetCardinality()
		row[1] = x
		total += x
		if counter <= count {
			rows = append(rows, row)
		} else {
			other += x
		}
		counter++
	}
	for i := range rows {
		percentage := float64(rows[i][1].(uint64)) / float64(total) * 100
		rows[i][1] = fmt.Sprintf("%10d", rows[i][1])
		rows[i][2] = fmt.Sprintf("%12.2f", percentage)
	}
	if other > 0 {
		otherRow := make([]driver.Value, 3)
		otherRow[0] = "OTHER:"
		otherRow[1] = fmt.Sprintf("%10d", other)
		otherRow[2] = fmt.Sprintf("%12.2f", float64(other)/float64(total)*100)
		rows = append(rows, otherRow)

	}
	row := make([]driver.Value, 3)
	row[0] = "TOTAL:"
	row[1] = fmt.Sprintf("%10d", total)
	row[2] = fmt.Sprintf("%12.2f", 100.0)
	rows = append(rows, row)
	return
}

// Sum - Sum aggregate.
func (p *Projector) Sum(table, field string) (sum int64, count uint64, err error) {

	r, errx := p.getAggregateResult(table, field)
	if errx != nil {
		err = errx
		return
	}
	sum, count = r.Sum(r.GetExistenceBitmap())
	return
}

// Min - Min aggregate.
func (p *Projector) Min(table, field string) (min int64, err error) {
	return p.minMax(true, table, field)
}

// Max - Max aggregate.
func (p *Projector) Max(table, field string) (max int64, err error) {
	return p.minMax(false, table, field)
}

func (p *Projector) minMax(isMin bool, table, field string) (minmax int64, err error) {

	r, errx := p.getAggregateResult(table, field)
	if errx != nil {
		err = errx
		return
	}

	if isMin {
		minmax = r.MinMax(0, roaring64.MIN, r.GetExistenceBitmap())
	} else {
		minmax = r.MinMax(0, roaring64.MAX, r.GetExistenceBitmap())
	}
	return
}

func (p *Projector) getAggregateResult(table, field string) (result *roaring64.BSI, err error) {

	bsiResults, bitmapResults, errx := p.retrieveBitmapResults(p.foundSets, p.projAttributes, false)
	if errx != nil {
		err = errx
		return
	}

	var ok bool
	result, ok = bsiResults[table][field]
	if !ok {
		if _, ok2 := bitmapResults[table][field]; ok2 {
			err = fmt.Errorf("Cannot aggregate non-BSI field '%s'", field)
			return
		}
		err = fmt.Errorf("Cannot locate results for field '%s'", field)
		return
	}
	var attr *Attribute
	for _, v := range p.projAttributes {
		if v.FieldName == field {
			attr = v
			break
		}
	}
	if attr == nil { // Should never be nil
		err = fmt.Errorf("Cannot locate attribute for field '%s'", field)
		return
	}
	return
}

// Handle boundary condition where a range of column IDs could span multiple partitions.
func (p *Projector) getPartitionedStrings(attr *Attribute, colIDs []uint64) (map[interface{}]interface{}, error) {

	lBatch := make(map[interface{}]interface{}, len(colIDs))
	if len(colIDs) == 0 {
		return lBatch, nil
	}
	startPartition := time.Unix(0, int64(colIDs[0]))
	endPartition := time.Unix(0, int64(colIDs[len(colIDs)-1]))

	if startPartition.Equal(endPartition) { // Everything in one partition
		lookupIndex := stringsPath(attr.Parent, attr.FieldName, "strings", startPartition)
		for _, colID := range colIDs {
			lBatch[colID] = ""
		}
		return p.connection.KVStore.BatchLookup(lookupIndex, lBatch, true)
	}

	batch := make(map[interface{}]interface{})
	for _, colID := range colIDs {
		endPartition = time.Unix(0, int64(colID))
		if !endPartition.Equal(startPartition) {
			lookupIndex := stringsPath(attr.Parent, attr.FieldName, "strings", startPartition)
			b, err := p.connection.KVStore.BatchLookup(lookupIndex, batch, true)
			if err != nil {
				return nil, fmt.Errorf("BatchLookup error for [%s] - %v", lookupIndex, err)
			}
			for k, v := range b {
				lBatch[k] = v
			}
			batch = make(map[interface{}]interface{})
			startPartition = endPartition
		}
		batch[colID] = ""
	}
	lookupIndex := stringsPath(attr.Parent, attr.FieldName, "strings", startPartition)
	b, err := p.connection.KVStore.BatchLookup(lookupIndex, batch, true)
	if err != nil {
		return nil, fmt.Errorf("BatchLookup error for [%s] - %v", lookupIndex, err)
	}
	for k, v := range b {
		lBatch[k] = v
	}
	return lBatch, nil
}

func stringsPath(table *Table, field, path string, ts time.Time) string {

	lookupPath := fmt.Sprintf("%s/%s/%s,%s", table.Name, field, path, ts.Format(timeFmt))
	if table.TimeQuantumType == "YMDH" {
		key := fmt.Sprintf("%s/%s/%s", table.Name, field, ts.Format(timeFmt))
		fpath := fmt.Sprintf("/%s/%s/%s/%s/%s", table.Name, field, path,
				fmt.Sprintf("%d%02d%02d", ts.Year(), ts.Month(), ts.Day()), ts.Format(timeFmt))
		lookupPath = key + "," + fpath
	}
	return lookupPath
}
