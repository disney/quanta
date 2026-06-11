package source

import (
	"testing"

	"github.com/disney/quanta/qlbridge/plan"
	"github.com/disney/quanta/qlbridge/rel"
	"github.com/disney/quanta/qlbridge/value"
)

func TestCountStarJoinMessageIsScalar(t *testing.T) {
	msg := newCountStarJoinMessage(12)

	if got := len(msg.Values()); got != 1 {
		t.Fatalf("expected one externally visible count value, got %d", got)
	}
	if got := msg.Values()[0]; got != "12" {
		t.Fatalf("expected count value 12, got %v", got)
	}
	if val, ok := msg.Get("count(*)"); !ok || val.ToString() != "12" {
		t.Fatalf("expected count(*) column to resolve to 12, got %v ok=%v", val, ok)
	}
}

func TestNormalizeCountStarJoinProjection(t *testing.T) {
	stmt, err := rel.ParseSql("select count(*) from customers_qa as c outer join orders_qa as o on o.cust_id == c.cust_id")
	if err != nil {
		t.Fatal(err)
	}

	proj := rel.NewProjection()
	proj.AddColumnShort("count(*)", value.StringType)
	proj.AddColumnShort("count(*)", value.StringType)
	ctx := &plan.Context{
		Stmt:       stmt,
		Projection: plan.NewProjectionStatic(proj),
	}

	normalizeCountStarJoinProjection(ctx)

	if got := len(ctx.Projection.Proj.Columns); got != 1 {
		t.Fatalf("expected duplicate count(*) join projection to collapse to one column, got %d", got)
	}
	if got := ctx.Projection.Proj.Columns[0].As; got != "count(*)" {
		t.Fatalf("expected count(*) projection, got %s", got)
	}
}
