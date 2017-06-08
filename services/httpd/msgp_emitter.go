package httpd

import (
	"io"
	"net/http"

	"github.com/influxdata/influxdb/influxql"
	"github.com/tinylib/msgp/msgp"
)

type messagePackEncoder struct {
	Epoch     string
	ChunkSize int
}

func (e *messagePackEncoder) ContentType() string {
	return "application/x-msgpack"
}

func (e *messagePackEncoder) Encode(w io.Writer, results <-chan *influxql.ResultSet) {
	var convertToEpoch func(row *influxql.Row)
	if e.Epoch != "" {
		convertToEpoch = epochConverter(e.Epoch)
	}
	values := make([][]interface{}, 0, e.ChunkSize)

	enc := msgp.NewWriter(w)
	for result := range results {
		var messages []Message
		if len(result.Messages) > 0 {
			messages = make([]Message, len(result.Messages))
			for i, m := range result.Messages {
				messages[i].Level = m.Level
				messages[i].Text = m.Text
			}
		}
		header := ResultHeader{
			ID:       result.ID,
			Messages: messages,
		}
		if result.Err != nil {
			err := result.Err.Error()
			header.Error = &err
		}
		header.EncodeMsg(enc)

		for series := range result.SeriesCh() {
			enc.WriteInt(1)

			if series.Err != nil {
				err := series.Err.Error()
				header := SeriesError{Error: err}
				header.EncodeMsg(enc)

				enc.Flush()
				if w, ok := w.(http.Flusher); ok {
					w.Flush()
				}
				continue
			}

			header := SeriesHeader{
				Name:    &series.Name,
				Tags:    series.Tags.KeyValues(),
				Columns: series.Columns,
			}
			header.EncodeMsg(enc)

			for row := range series.RowCh() {
				if row.Err != nil {
					enc.WriteInt(len(values) + 1)
					for _, v := range values {
						val := Row{Value: v}
						val.EncodeMsg(enc)
					}
					values = values[:0]

					err := RowError{Error: row.Err.Error()}
					err.EncodeMsg(enc)
					continue
				}

				if convertToEpoch != nil {
					convertToEpoch(&row)
				}

				values = append(values, row.Values)
				if len(values) == cap(values) {
					enc.WriteInt(len(values))
					for _, v := range values {
						val := Row{Value: v}
						val.EncodeMsg(enc)
					}
					values = values[:0]

					enc.Flush()
					if w, ok := w.(http.Flusher); ok {
						w.Flush()
					}
				}
			}

			if len(values) > 0 {
				enc.WriteInt(len(values))
				for _, v := range values {
					val := Row{Value: v}
					val.EncodeMsg(enc)
				}
				values = values[:0]
			}
			enc.WriteInt(0)

			enc.Flush()
			if w, ok := w.(http.Flusher); ok {
				w.Flush()
			}
		}
		enc.WriteInt(0)

		enc.Flush()
		if w, ok := w.(http.Flusher); ok {
			w.Flush()
		}
	}
}

func (e *messagePackEncoder) Error(w io.Writer, err error) {
	enc := msgp.NewWriter(w)
	enc.WriteMapHeader(1)
	enc.WriteString("error")
	enc.WriteString(err.Error())
	enc.Flush()
}
