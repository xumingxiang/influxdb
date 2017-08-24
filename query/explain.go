package query

import (
	"fmt"
	"strings"

	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/models"
)

func (p *preparedStatement) Explain() (models.Rows, error) {
	// Determine the cost of all iterators created as part of this plan.
	ic := &explainIteratorCreator{ic: p.ic}
	p.ic = ic
	itrs, _, err := p.Select()
	p.ic = ic.ic

	if err != nil {
		return nil, err
	}
	Iterators(itrs).Close()

	var plan []string
	for _, node := range ic.nodes {
		expr := "<nil>"
		if node.Expr != nil {
			expr = node.Expr.String()
		}
		plan = append(plan, fmt.Sprintf("EXPRESSION: %s", expr))
		if len(node.Aux) != 0 {
			refs := make([]string, len(node.Aux))
			for i, ref := range node.Aux {
				refs[i] = ref.String()
			}
			plan = append(plan, fmt.Sprintf("AUXILIARY FIELDS: %s", strings.Join(refs, ", ")))
		}
		plan = append(plan, fmt.Sprintf("NUMBER OF SHARDS: %d", node.Cost.NumShards))
		plan = append(plan, fmt.Sprintf("NUMBER OF SERIES: %d", node.Cost.NumSeries))
		plan = append(plan, fmt.Sprintf("NUMBER OF FILES: %d", node.Cost.NumFiles))
	}

	// Look through the potential costs incurred by creating this query.
	row := &models.Row{
		Columns: []string{"QUERY PLAN"},
	}
	for _, s := range plan {
		row.Values = append(row.Values, []interface{}{s})
	}
	return models.Rows{row}, nil
}

type planNode struct {
	Expr influxql.Expr
	Aux  []influxql.VarRef
	Cost IteratorCost
}

type explainIteratorCreator struct {
	ic    IteratorCreator
	nodes []planNode
}

func (e *explainIteratorCreator) CreateIterator(m *influxql.Measurement, opt IteratorOptions) (Iterator, error) {
	cost, err := e.ic.IteratorCost(m, opt)
	if err != nil {
		return nil, err
	}
	e.nodes = append(e.nodes, planNode{
		Expr: opt.Expr,
		Aux:  opt.Aux,
		Cost: cost,
	})
	return &nilFloatIterator{}, nil
}

func (e *explainIteratorCreator) IteratorCost(m *influxql.Measurement, opt IteratorOptions) (IteratorCost, error) {
	return e.ic.IteratorCost(m, opt)
}
