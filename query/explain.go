package query

import (
	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/models"
)

func (p *preparedStatement) Explain() (models.Rows, error) {
	// Find the measurements in the top level of the query.
	measurements := make([]*influxql.Measurement, 0, len(p.stmt.Sources))
	for _, source := range p.stmt.Sources {
		switch source := source.(type) {
		case *influxql.Measurement:
			measurements = append(measurements, source)
		}
	}

	// Determine the query cost for each measurement.
	rows := make([]*models.Row, 0, len(measurements))
	for _, measurement := range measurements {
		cost, err := p.ic.IteratorCost(measurement, p.opt)
		if err != nil {
			return nil, err
		}

		row := &models.Row{
			Name:    measurement.Name,
			Columns: []string{"name", "value"},
		}
		row.Values = append(row.Values, []interface{}{"number of shards", cost.NumShards})
		rows = append(rows, row)
	}
	return models.Rows(rows), nil
}
