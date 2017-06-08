package httpd

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"expvar"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"runtime/debug"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/bmizerany/pat"
	"github.com/dgrijalva/jwt-go"
	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/monitor"
	"github.com/influxdata/influxdb/monitor/diagnostics"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxdb/uuid"
	"github.com/uber-go/zap"
)

const (
	// DefaultChunkSize specifies the maximum number of points that will
	// be read before sending results back to the engine.
	//
	// This has no relation to the number of bytes that are returned.
	DefaultChunkSize = 10000

	DefaultDebugRequestsInterval = 10 * time.Second

	MaxDebugRequestsInterval = 6 * time.Hour
)

// AuthenticationMethod defines the type of authentication used.
type AuthenticationMethod int

// Supported authentication methods.
const (
	// Authenticate using basic authentication.
	UserAuthentication AuthenticationMethod = iota

	// Authenticate with jwt.
	BearerAuthentication
)

// TODO: Check HTTP response codes: 400, 401, 403, 409.

// Route specifies how to handle a HTTP verb for a given endpoint.
type Route struct {
	Name           string
	Method         string
	Pattern        string
	Gzipped        bool
	LoggingEnabled bool
	HandlerFunc    interface{}
}

// Handler represents an HTTP handler for the InfluxDB server.
type Handler struct {
	mux       *pat.PatternServeMux
	Version   string
	BuildType string

	MetaClient interface {
		Database(name string) *meta.DatabaseInfo
		Databases() []meta.DatabaseInfo
		Authenticate(username, password string) (ui meta.User, err error)
		User(username string) (meta.User, error)
		AdminUserExists() bool
	}

	QueryAuthorizer interface {
		AuthorizeQuery(u meta.User, query *influxql.Query, database string) error
	}

	WriteAuthorizer interface {
		AuthorizeWrite(username, database string) error
	}

	QueryExecutor *query.QueryExecutor

	Monitor interface {
		Statistics(tags map[string]string) ([]*monitor.Statistic, error)
		Diagnostics() (map[string]*diagnostics.Diagnostics, error)
	}

	PointsWriter interface {
		WritePoints(database, retentionPolicy string, consistencyLevel models.ConsistencyLevel, user meta.User, points []models.Point) error
	}

	Config    *Config
	Logger    zap.Logger
	CLFLogger *log.Logger
	stats     *Statistics

	requestTracker *RequestTracker
}

// NewHandler returns a new instance of handler with routes.
func NewHandler(c Config) *Handler {
	h := &Handler{
		mux:            pat.New(),
		Config:         &c,
		Logger:         zap.New(zap.NullEncoder()),
		CLFLogger:      log.New(os.Stderr, "[httpd] ", 0),
		stats:          &Statistics{},
		requestTracker: NewRequestTracker(),
	}

	h.AddRoutes([]Route{
		Route{
			"query-options", // Satisfy CORS checks.
			"OPTIONS", "/query", false, true, h.serveOptions,
		},
		Route{
			"query", // Query serving route.
			"GET", "/query", true, true, h.serveQuery,
		},
		Route{
			"query", // Query serving route.
			"POST", "/query", true, true, h.serveQuery,
		},
		Route{
			"write-options", // Satisfy CORS checks.
			"OPTIONS", "/write", false, true, h.serveOptions,
		},
		Route{
			"write", // Data-ingest route.
			"POST", "/write", true, true, h.serveWrite,
		},
		Route{ // Ping
			"ping",
			"GET", "/ping", false, true, h.servePing,
		},
		Route{ // Ping
			"ping-head",
			"HEAD", "/ping", false, true, h.servePing,
		},
		Route{ // Ping w/ status
			"status",
			"GET", "/status", false, true, h.serveStatus,
		},
		Route{ // Ping w/ status
			"status-head",
			"HEAD", "/status", false, true, h.serveStatus,
		},
	}...)

	return h
}

// Statistics maintains statistics for the httpd service.
type Statistics struct {
	Requests                     int64
	CQRequests                   int64
	QueryRequests                int64
	WriteRequests                int64
	PingRequests                 int64
	StatusRequests               int64
	WriteRequestBytesReceived    int64
	QueryRequestBytesTransmitted int64
	PointsWrittenOK              int64
	PointsWrittenDropped         int64
	PointsWrittenFail            int64
	AuthenticationFailures       int64
	RequestDuration              int64
	QueryRequestDuration         int64
	WriteRequestDuration         int64
	ActiveRequests               int64
	ActiveWriteRequests          int64
	ClientErrors                 int64
	ServerErrors                 int64
	RecoveredPanics              int64
}

// Statistics returns statistics for periodic monitoring.
func (h *Handler) Statistics(tags map[string]string) []models.Statistic {
	return []models.Statistic{{
		Name: "httpd",
		Tags: tags,
		Values: map[string]interface{}{
			statRequest:                      atomic.LoadInt64(&h.stats.Requests),
			statQueryRequest:                 atomic.LoadInt64(&h.stats.QueryRequests),
			statWriteRequest:                 atomic.LoadInt64(&h.stats.WriteRequests),
			statPingRequest:                  atomic.LoadInt64(&h.stats.PingRequests),
			statStatusRequest:                atomic.LoadInt64(&h.stats.StatusRequests),
			statWriteRequestBytesReceived:    atomic.LoadInt64(&h.stats.WriteRequestBytesReceived),
			statQueryRequestBytesTransmitted: atomic.LoadInt64(&h.stats.QueryRequestBytesTransmitted),
			statPointsWrittenOK:              atomic.LoadInt64(&h.stats.PointsWrittenOK),
			statPointsWrittenDropped:         atomic.LoadInt64(&h.stats.PointsWrittenDropped),
			statPointsWrittenFail:            atomic.LoadInt64(&h.stats.PointsWrittenFail),
			statAuthFail:                     atomic.LoadInt64(&h.stats.AuthenticationFailures),
			statRequestDuration:              atomic.LoadInt64(&h.stats.RequestDuration),
			statQueryRequestDuration:         atomic.LoadInt64(&h.stats.QueryRequestDuration),
			statWriteRequestDuration:         atomic.LoadInt64(&h.stats.WriteRequestDuration),
			statRequestsActive:               atomic.LoadInt64(&h.stats.ActiveRequests),
			statWriteRequestsActive:          atomic.LoadInt64(&h.stats.ActiveWriteRequests),
			statClientError:                  atomic.LoadInt64(&h.stats.ClientErrors),
			statServerError:                  atomic.LoadInt64(&h.stats.ServerErrors),
			statRecoveredPanics:              atomic.LoadInt64(&h.stats.RecoveredPanics),
		},
	}}
}

// AddRoutes sets the provided routes on the handler.
func (h *Handler) AddRoutes(routes ...Route) {
	for _, r := range routes {
		var handler http.Handler

		// If it's a handler func that requires authorization, wrap it in authentication
		if hf, ok := r.HandlerFunc.(func(http.ResponseWriter, *http.Request, meta.User)); ok {
			handler = authenticate(hf, h, h.Config.AuthEnabled)
		}

		// This is a normal handler signature and does not require authentication
		if hf, ok := r.HandlerFunc.(func(http.ResponseWriter, *http.Request)); ok {
			handler = http.HandlerFunc(hf)
		}

		if r.Gzipped {
			handler = gzipFilter(handler)
		}
		handler = cors(handler)
		handler = requestID(handler)
		if h.Config.LogEnabled && r.LoggingEnabled {
			handler = h.logging(handler, r.Name)
		}
		handler = h.recovery(handler, r.Name) // make sure recovery is always last

		h.mux.Add(r.Method, r.Pattern, handler)
	}
}

// ServeHTTP responds to HTTP request to the handler.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	atomic.AddInt64(&h.stats.Requests, 1)
	atomic.AddInt64(&h.stats.ActiveRequests, 1)
	defer atomic.AddInt64(&h.stats.ActiveRequests, -1)
	start := time.Now()

	// Add version and build header to all InfluxDB requests.
	w.Header().Add("X-Influxdb-Version", h.Version)
	w.Header().Add("X-Influxdb-Build", h.BuildType)

	if strings.HasPrefix(r.URL.Path, "/debug/pprof") && h.Config.PprofEnabled {
		h.handleProfiles(w, r)
	} else if strings.HasPrefix(r.URL.Path, "/debug/vars") {
		h.serveExpvar(w, r)
	} else if strings.HasPrefix(r.URL.Path, "/debug/requests") {
		h.serveDebugRequests(w, r)
	} else {
		h.mux.ServeHTTP(w, r)
	}

	atomic.AddInt64(&h.stats.RequestDuration, time.Since(start).Nanoseconds())
}

// writeHeader writes the provided status code in the response, and
// updates relevant http error statistics.
func (h *Handler) writeHeader(w http.ResponseWriter, code int) {
	switch code / 100 {
	case 4:
		atomic.AddInt64(&h.stats.ClientErrors, 1)
	case 5:
		atomic.AddInt64(&h.stats.ServerErrors, 1)
	}
	w.WriteHeader(code)
}

// serveQuery parses an incoming query and, if valid, executes the query.
func (h *Handler) serveQuery(w http.ResponseWriter, r *http.Request, user meta.User) {
	atomic.AddInt64(&h.stats.QueryRequests, 1)
	defer func(start time.Time) {
		atomic.AddInt64(&h.stats.QueryRequestDuration, time.Since(start).Nanoseconds())
	}(time.Now())
	h.requestTracker.Add(r, user)

	// Initialize an encoder for us to use to encode the response.
	enc := NewEncoder(r, h.Config)

	// Retrieve the node id the query should be executed on.
	nodeID, _ := strconv.ParseUint(r.FormValue("node_id"), 10, 64)

	var qr io.Reader
	// Attempt to read the form value from the "q" form value.
	if qp := strings.TrimSpace(r.FormValue("q")); qp != "" {
		qr = strings.NewReader(qp)
	} else if r.MultipartForm != nil && r.MultipartForm.File != nil {
		// If we have a multipart/form-data, try to retrieve a file from 'q'.
		if fhs := r.MultipartForm.File["q"]; len(fhs) > 0 {
			f, err := fhs[0].Open()
			if err != nil {
				h.httpError(w, enc, err.Error(), http.StatusBadRequest)
				return
			}
			defer f.Close()
			qr = f
		}
	}

	if qr == nil {
		h.httpError(w, enc, `missing required parameter "q"`, http.StatusBadRequest)
		return
	}

	p := influxql.NewParser(qr)
	db := r.FormValue("db")

	// Sanitize the request query params so it doesn't show up in the response logger.
	// Do this before anything else so a parsing error doesn't leak passwords.
	sanitize(r)

	// Parse the parameters
	rawParams := r.FormValue("params")
	if rawParams != "" {
		var params map[string]interface{}
		decoder := json.NewDecoder(strings.NewReader(rawParams))
		decoder.UseNumber()
		if err := decoder.Decode(&params); err != nil {
			h.httpError(w, enc, "error parsing query parameters: "+err.Error(), http.StatusBadRequest)
			return
		}

		// Convert json.Number into int64 and float64 values
		for k, v := range params {
			if v, ok := v.(json.Number); ok {
				var err error
				if strings.Contains(string(v), ".") {
					params[k], err = v.Float64()
				} else {
					params[k], err = v.Int64()
				}

				if err != nil {
					h.httpError(w, enc, "error parsing json value: "+err.Error(), http.StatusBadRequest)
					return
				}
			}
		}
		p.SetParams(params)
	}

	// Parse query from query string.
	q, err := p.ParseQuery()
	if err != nil {
		h.httpError(w, enc, "error parsing query: "+err.Error(), http.StatusBadRequest)
		return
	}

	// Check authorization.
	if h.Config.AuthEnabled {
		if err := h.QueryAuthorizer.AuthorizeQuery(user, q, db); err != nil {
			if err, ok := err.(meta.ErrAuthorize); ok {
				h.Logger.Info(fmt.Sprintf("Unauthorized request | user: %q | query: %q | database %q", err.User, err.Query.String(), err.Database))
			}
			h.httpError(w, enc, "error authorizing query: "+err.Error(), http.StatusForbidden)
			return
		}
	}

	// Parse whether this is an async command.
	async := r.FormValue("async") == "true"

	opts := query.ExecutionOptions{
		Database: db,
		ReadOnly: r.Method == "GET",
		NodeID:   nodeID,
	}

	if h.Config.AuthEnabled {
		// The current user determines the authorized actions.
		opts.Authorizer = user
	} else {
		// Auth is disabled, so allow everything.
		opts.Authorizer = query.OpenAuthorizer{}
	}

	// Make sure if the client disconnects we signal the query to abort
	var closing chan struct{}
	if !async {
		closing = make(chan struct{})
		if notifier, ok := w.(http.CloseNotifier); ok {
			// CloseNotify() is not guaranteed to send a notification when the query
			// is closed. Use this channel to signal that the query is finished to
			// prevent lingering goroutines that may be stuck.
			done := make(chan struct{})
			defer close(done)

			notify := notifier.CloseNotify()
			go func() {
				// Wait for either the request to finish
				// or for the client to disconnect
				select {
				case <-done:
				case <-notify:
					close(closing)
				}
			}()
			opts.AbortCh = done
		} else {
			defer close(closing)
		}
	}

	// Execute query.
	results := h.QueryExecutor.ExecuteQuery(q, opts, closing)

	// If we are running in async mode, open a goroutine to drain the results
	// and return with a StatusNoContent.
	if async {
		go h.async(q, results)
		h.writeHeader(w, http.StatusNoContent)
		return
	}

	// Status header is OK once this point is reached.
	// Attempt to flush the header immediately so the client gets the header information
	// and knows the query was accepted.
	w.Header().Set("Content-Type", enc.ContentType())
	h.writeHeader(w, http.StatusOK)
	if w, ok := w.(http.Flusher); ok {
		w.Flush()
	}

	// Read the results and encode them in the proper structure.
	enc.Encode(w, results)
}

// async drains the results from an async query and logs a message if it fails.
func (h *Handler) async(q *influxql.Query, results <-chan *query.ResultSet) {
	for r := range results {
		// Drain the results and do nothing with them.
		// If it fails, log the failure so there is at least a record of it.
		if r.Err != nil {
			// Do not log when a statement was not executed since there would
			// have been an earlier error that was already logged.
			if r.Err == query.ErrNotExecuted {
				continue
			}
			h.Logger.Info(fmt.Sprintf("error while running async query: %s: %s", q, r.Err))
		}

		for series := range r.SeriesCh() {
			for row := range series.RowCh() {
				if row.Err != nil {
					h.Logger.Info(fmt.Sprintf("error while running async query: %s: %s", q, row.Err))
				}
			}
		}
	}
}

// serveWrite receives incoming series data in line protocol format and writes it to the database.
func (h *Handler) serveWrite(w http.ResponseWriter, r *http.Request, user meta.User) {
	atomic.AddInt64(&h.stats.WriteRequests, 1)
	atomic.AddInt64(&h.stats.ActiveWriteRequests, 1)
	defer func(start time.Time) {
		atomic.AddInt64(&h.stats.ActiveWriteRequests, -1)
		atomic.AddInt64(&h.stats.WriteRequestDuration, time.Since(start).Nanoseconds())
	}(time.Now())
	h.requestTracker.Add(r, user)

	database := r.URL.Query().Get("db")
	if database == "" {
		h.httpError(w, NewEncoder(r, h.Config), "database is required", http.StatusBadRequest)
		return
	}

	if di := h.MetaClient.Database(database); di == nil {
		h.httpError(w, NewEncoder(r, h.Config), fmt.Sprintf("database not found: %q", database), http.StatusNotFound)
		return
	}

	if h.Config.AuthEnabled {
		if user == nil {
			h.httpError(w, NewEncoder(r, h.Config), fmt.Sprintf("user is required to write to database %q", database), http.StatusForbidden)
			return
		}

		if err := h.WriteAuthorizer.AuthorizeWrite(user.ID(), database); err != nil {
			h.httpError(w, NewEncoder(r, h.Config), fmt.Sprintf("%q user is not authorized to write to database %q", user.ID(), database), http.StatusForbidden)
			return
		}
	}

	body := r.Body
	if h.Config.MaxBodySize > 0 {
		body = truncateReader(body, int64(h.Config.MaxBodySize))
	}

	// Handle gzip decoding of the body
	if r.Header.Get("Content-Encoding") == "gzip" {
		b, err := gzip.NewReader(r.Body)
		if err != nil {
			h.httpError(w, NewEncoder(r, h.Config), err.Error(), http.StatusBadRequest)
			return
		}
		defer b.Close()
		body = b
	}

	var bs []byte
	if r.ContentLength > 0 {
		if h.Config.MaxBodySize > 0 && r.ContentLength > int64(h.Config.MaxBodySize) {
			h.httpError(w, NewEncoder(r, h.Config), http.StatusText(http.StatusRequestEntityTooLarge), http.StatusRequestEntityTooLarge)
			return
		}

		// This will just be an initial hint for the gzip reader, as the
		// bytes.Buffer will grow as needed when ReadFrom is called
		bs = make([]byte, 0, r.ContentLength)
	}
	buf := bytes.NewBuffer(bs)

	_, err := buf.ReadFrom(body)
	if err != nil {
		if err == errTruncated {
			h.httpError(w, NewEncoder(r, h.Config), http.StatusText(http.StatusRequestEntityTooLarge), http.StatusRequestEntityTooLarge)
			return
		}

		if h.Config.WriteTracing {
			h.Logger.Info("Write handler unable to read bytes from request body")
		}
		h.httpError(w, NewEncoder(r, h.Config), err.Error(), http.StatusBadRequest)
		return
	}
	atomic.AddInt64(&h.stats.WriteRequestBytesReceived, int64(buf.Len()))

	if h.Config.WriteTracing {
		h.Logger.Info(fmt.Sprintf("Write body received by handler: %s", buf.Bytes()))
	}

	points, parseError := models.ParsePointsWithPrecision(buf.Bytes(), time.Now().UTC(), r.URL.Query().Get("precision"))
	// Not points parsed correctly so return the error now
	if parseError != nil && len(points) == 0 {
		if parseError.Error() == "EOF" {
			h.writeHeader(w, http.StatusOK)
			return
		}
		h.httpError(w, NewEncoder(r, h.Config), parseError.Error(), http.StatusBadRequest)
		return
	}

	// Determine required consistency level.
	level := r.URL.Query().Get("consistency")
	consistency := models.ConsistencyLevelOne
	if level != "" {
		var err error
		consistency, err = models.ParseConsistencyLevel(level)
		if err != nil {
			h.httpError(w, NewEncoder(r, h.Config), err.Error(), http.StatusBadRequest)
			return
		}
	}

	// Write points.
	if err := h.PointsWriter.WritePoints(database, r.URL.Query().Get("rp"), consistency, user, points); influxdb.IsClientError(err) {
		atomic.AddInt64(&h.stats.PointsWrittenFail, int64(len(points)))
		h.httpError(w, NewEncoder(r, h.Config), err.Error(), http.StatusBadRequest)
		return
	} else if influxdb.IsAuthorizationError(err) {
		atomic.AddInt64(&h.stats.PointsWrittenFail, int64(len(points)))
		h.httpError(w, NewEncoder(r, h.Config), err.Error(), http.StatusForbidden)
		return
	} else if werr, ok := err.(tsdb.PartialWriteError); ok {
		atomic.AddInt64(&h.stats.PointsWrittenOK, int64(len(points)-werr.Dropped))
		atomic.AddInt64(&h.stats.PointsWrittenDropped, int64(werr.Dropped))
		h.httpError(w, NewEncoder(r, h.Config), werr.Error(), http.StatusBadRequest)
		return
	} else if err != nil {
		atomic.AddInt64(&h.stats.PointsWrittenFail, int64(len(points)))
		h.httpError(w, NewEncoder(r, h.Config), err.Error(), http.StatusInternalServerError)
		return
	} else if parseError != nil {
		// We wrote some of the points
		atomic.AddInt64(&h.stats.PointsWrittenOK, int64(len(points)))
		// The other points failed to parse which means the client sent invalid line protocol.  We return a 400
		// response code as well as the lines that failed to parse.
		h.httpError(w, NewEncoder(r, h.Config), tsdb.PartialWriteError{Reason: parseError.Error()}.Error(), http.StatusBadRequest)
		return
	}

	atomic.AddInt64(&h.stats.PointsWrittenOK, int64(len(points)))
	h.writeHeader(w, http.StatusNoContent)
}

// serveOptions returns an empty response to comply with OPTIONS pre-flight requests
func (h *Handler) serveOptions(w http.ResponseWriter, r *http.Request) {
	h.writeHeader(w, http.StatusNoContent)
}

// servePing returns a simple response to let the client know the server is running.
func (h *Handler) servePing(w http.ResponseWriter, r *http.Request) {
	atomic.AddInt64(&h.stats.PingRequests, 1)
	h.writeHeader(w, http.StatusNoContent)
}

// serveStatus has been deprecated.
func (h *Handler) serveStatus(w http.ResponseWriter, r *http.Request) {
	h.Logger.Info("WARNING: /status has been deprecated.  Use /ping instead.")
	atomic.AddInt64(&h.stats.StatusRequests, 1)
	h.writeHeader(w, http.StatusNoContent)
}

// convertToEpoch converts result timestamps from time.Time to the specified epoch.
func convertToEpoch(r *query.Result, epoch string) {
	divisor := int64(1)

	switch epoch {
	case "u":
		divisor = int64(time.Microsecond)
	case "ms":
		divisor = int64(time.Millisecond)
	case "s":
		divisor = int64(time.Second)
	case "m":
		divisor = int64(time.Minute)
	case "h":
		divisor = int64(time.Hour)
	}

	for _, s := range r.Series {
		for _, v := range s.Values {
			if ts, ok := v[0].(time.Time); ok {
				v[0] = ts.UnixNano() / divisor
			}
		}
	}
}

// serveExpvar serves internal metrics in /debug/vars format over HTTP.
func (h *Handler) serveExpvar(w http.ResponseWriter, r *http.Request) {
	// Retrieve statistics from the monitor.
	stats, err := h.Monitor.Statistics(nil)
	if err != nil {
		h.httpError(w, NewEncoder(r, h.Config), err.Error(), http.StatusInternalServerError)
		return
	}

	// Retrieve diagnostics from the monitor.
	diags, err := h.Monitor.Diagnostics()
	if err != nil {
		h.httpError(w, NewEncoder(r, h.Config), err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")

	first := true
	if val, ok := diags["system"]; ok {
		jv, err := parseSystemDiagnostics(val)
		if err != nil {
			h.httpError(w, NewEncoder(r, h.Config), err.Error(), http.StatusInternalServerError)
			return
		}

		data, err := json.Marshal(jv)
		if err != nil {
			h.httpError(w, NewEncoder(r, h.Config), err.Error(), http.StatusInternalServerError)
			return
		}

		first = false
		fmt.Fprintln(w, "{")
		fmt.Fprintf(w, "\"system\": %s", data)
	} else {
		fmt.Fprintln(w, "{")
	}

	if val := expvar.Get("cmdline"); val != nil {
		if !first {
			fmt.Fprintln(w, ",")
		}
		first = false
		fmt.Fprintf(w, "\"cmdline\": %s", val)
	}
	if val := expvar.Get("memstats"); val != nil {
		if !first {
			fmt.Fprintln(w, ",")
		}
		first = false
		fmt.Fprintf(w, "\"memstats\": %s", val)
	}

	for _, s := range stats {
		val, err := json.Marshal(s)
		if err != nil {
			continue
		}

		// Very hackily create a unique key.
		buf := bytes.NewBufferString(s.Name)
		if path, ok := s.Tags["path"]; ok {
			fmt.Fprintf(buf, ":%s", path)
			if id, ok := s.Tags["id"]; ok {
				fmt.Fprintf(buf, ":%s", id)
			}
		} else if bind, ok := s.Tags["bind"]; ok {
			if proto, ok := s.Tags["proto"]; ok {
				fmt.Fprintf(buf, ":%s", proto)
			}
			fmt.Fprintf(buf, ":%s", bind)
		} else if database, ok := s.Tags["database"]; ok {
			fmt.Fprintf(buf, ":%s", database)
			if rp, ok := s.Tags["retention_policy"]; ok {
				fmt.Fprintf(buf, ":%s", rp)
				if name, ok := s.Tags["name"]; ok {
					fmt.Fprintf(buf, ":%s", name)
				}
				if dest, ok := s.Tags["destination"]; ok {
					fmt.Fprintf(buf, ":%s", dest)
				}
			}
		}
		key := buf.String()

		if !first {
			fmt.Fprintln(w, ",")
		}
		first = false
		fmt.Fprintf(w, "%q: ", key)
		w.Write(bytes.TrimSpace(val))
	}
	fmt.Fprintln(w, "\n}")
}

// serveDebugRequests will track requests for a period of time.
func (h *Handler) serveDebugRequests(w http.ResponseWriter, r *http.Request) {
	var d time.Duration
	if s := r.URL.Query().Get("seconds"); s == "" {
		d = DefaultDebugRequestsInterval
	} else if seconds, err := strconv.ParseInt(s, 10, 64); err != nil {
		h.httpError(w, NewEncoder(r, h.Config), err.Error(), http.StatusBadRequest)
		return
	} else {
		d = time.Duration(seconds) * time.Second
		if d > MaxDebugRequestsInterval {
			h.httpError(w, NewEncoder(r, h.Config), fmt.Sprintf("exceeded maximum interval time: %s > %s",
				influxql.FormatDuration(d),
				influxql.FormatDuration(MaxDebugRequestsInterval)),
				http.StatusBadRequest)
			return
		}
	}

	var closing <-chan bool
	if notifier, ok := w.(http.CloseNotifier); ok {
		closing = notifier.CloseNotify()
	}

	profile := h.requestTracker.TrackRequests()

	timer := time.NewTimer(d)
	select {
	case <-timer.C:
		profile.Stop()
	case <-closing:
		// Connection was closed early.
		profile.Stop()
		timer.Stop()
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Add("Connection", "close")

	fmt.Fprintln(w, "{")
	first := true
	for req, st := range profile.Requests {
		val, err := json.Marshal(st)
		if err != nil {
			continue
		}

		if !first {
			fmt.Fprintln(w, ",")
		}
		first = false
		fmt.Fprintf(w, "%q: ", req.String())
		w.Write(bytes.TrimSpace(val))
	}
	fmt.Fprintln(w, "\n}")
}

// parseSystemDiagnostics converts the system diagnostics into an appropriate
// format for marshaling to JSON in the /debug/vars format.
func parseSystemDiagnostics(d *diagnostics.Diagnostics) (map[string]interface{}, error) {
	// We don't need PID in this case.
	m := map[string]interface{}{"currentTime": nil, "started": nil, "uptime": nil}
	for key := range m {
		// Find the associated column.
		ci := -1
		for i, col := range d.Columns {
			if col == key {
				ci = i
				break
			}
		}

		if ci == -1 {
			return nil, fmt.Errorf("unable to find column %q", key)
		}

		if len(d.Rows) < 1 || len(d.Rows[0]) <= ci {
			return nil, fmt.Errorf("no data for column %q", key)
		}

		var res interface{}
		switch v := d.Rows[0][ci].(type) {
		case time.Time:
			res = v
		case string:
			// Should be a string representation of a time.Duration
			d, err := time.ParseDuration(v)
			if err != nil {
				return nil, err
			}
			res = int64(d.Seconds())
		default:
			return nil, fmt.Errorf("value for column %q is not parsable (got %T)", key, v)
		}
		m[key] = res
	}
	return m, nil
}

// httpError writes an error to the client in a standard format.
func (h *Handler) httpError(w http.ResponseWriter, enc Encoder, errmsg string, code int) {
	if code == http.StatusUnauthorized {
		// If an unauthorized header will be sent back, add a WWW-Authenticate header
		// as an authorization challenge.
		w.Header().Set("WWW-Authenticate", fmt.Sprintf("Basic realm=\"%s\"", h.Config.Realm))
	} else if code/100 != 2 {
		sz := math.Min(float64(len(errmsg)), 1024.0)
		w.Header().Set("X-InfluxDB-Error", errmsg[:int(sz)])
	}

	h.writeHeader(w, code)
	enc.Error(w, errors.New(errmsg))
}

// Filters and filter helpers

type credentials struct {
	Method   AuthenticationMethod
	Username string
	Password string
	Token    string
}

// parseCredentials parses a request and returns the authentication credentials.
// The credentials may be present as URL query params, or as a Basic
// Authentication header.
// As params: http://127.0.0.1/query?u=username&p=password
// As basic auth: http://username:password@127.0.0.1
// As Bearer token in Authorization header: Bearer <JWT_TOKEN_BLOB>
func parseCredentials(r *http.Request) (*credentials, error) {
	q := r.URL.Query()

	// Check for username and password in URL params.
	if u, p := q.Get("u"), q.Get("p"); u != "" && p != "" {
		return &credentials{
			Method:   UserAuthentication,
			Username: u,
			Password: p,
		}, nil
	}

	// Check for the HTTP Authorization header.
	if s := r.Header.Get("Authorization"); s != "" {
		// Check for Bearer token.
		strs := strings.Split(s, " ")
		if len(strs) == 2 && strs[0] == "Bearer" {
			return &credentials{
				Method: BearerAuthentication,
				Token:  strs[1],
			}, nil
		}

		// Check for basic auth.
		if u, p, ok := r.BasicAuth(); ok {
			return &credentials{
				Method:   UserAuthentication,
				Username: u,
				Password: p,
			}, nil
		}
	}

	return nil, fmt.Errorf("unable to parse authentication credentials")
}

// authenticate wraps a handler and ensures that if user credentials are passed in
// an attempt is made to authenticate that user. If authentication fails, an error is returned.
//
// There is one exception: if there are no users in the system, authentication is not required. This
// is to facilitate bootstrapping of a system with authentication enabled.
func authenticate(inner func(http.ResponseWriter, *http.Request, meta.User), h *Handler, requireAuthentication bool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Return early if we are not authenticating
		if !requireAuthentication {
			inner(w, r, nil)
			return
		}
		var user meta.User

		// TODO corylanou: never allow this in the future without users
		if requireAuthentication && h.MetaClient.AdminUserExists() {
			creds, err := parseCredentials(r)
			if err != nil {
				atomic.AddInt64(&h.stats.AuthenticationFailures, 1)
				h.httpError(w, NewEncoder(r, h.Config), err.Error(), http.StatusUnauthorized)
				return
			}

			switch creds.Method {
			case UserAuthentication:
				if creds.Username == "" {
					atomic.AddInt64(&h.stats.AuthenticationFailures, 1)
					h.httpError(w, NewEncoder(r, h.Config), "username required", http.StatusUnauthorized)
					return
				}

				user, err = h.MetaClient.Authenticate(creds.Username, creds.Password)
				if err != nil {
					atomic.AddInt64(&h.stats.AuthenticationFailures, 1)
					h.httpError(w, NewEncoder(r, h.Config), "authorization failed", http.StatusUnauthorized)
					return
				}
			case BearerAuthentication:
				keyLookupFn := func(token *jwt.Token) (interface{}, error) {
					// Check for expected signing method.
					if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
						return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
					}
					return []byte(h.Config.SharedSecret), nil
				}

				// Parse and validate the token.
				token, err := jwt.Parse(creds.Token, keyLookupFn)
				if err != nil {
					h.httpError(w, NewEncoder(r, h.Config), err.Error(), http.StatusUnauthorized)
					return
				} else if !token.Valid {
					h.httpError(w, NewEncoder(r, h.Config), "invalid token", http.StatusUnauthorized)
					return
				}

				claims, ok := token.Claims.(jwt.MapClaims)
				if !ok {
					h.httpError(w, NewEncoder(r, h.Config), "problem authenticating token", http.StatusInternalServerError)
					h.Logger.Info("Could not assert JWT token claims as jwt.MapClaims")
					return
				}

				// Make sure an expiration was set on the token.
				if exp, ok := claims["exp"].(float64); !ok || exp <= 0.0 {
					h.httpError(w, NewEncoder(r, h.Config), "token expiration required", http.StatusUnauthorized)
					return
				}

				// Get the username from the token.
				username, ok := claims["username"].(string)
				if !ok {
					h.httpError(w, NewEncoder(r, h.Config), "username in token must be a string", http.StatusUnauthorized)
					return
				} else if username == "" {
					h.httpError(w, NewEncoder(r, h.Config), "token must contain a username", http.StatusUnauthorized)
					return
				}

				// Lookup user in the metastore.
				if user, err = h.MetaClient.User(username); err != nil {
					h.httpError(w, NewEncoder(r, h.Config), err.Error(), http.StatusUnauthorized)
					return
				} else if user == nil {
					h.httpError(w, NewEncoder(r, h.Config), meta.ErrUserNotFound.Error(), http.StatusUnauthorized)
					return
				}
			default:
				h.httpError(w, NewEncoder(r, h.Config), "unsupported authentication", http.StatusUnauthorized)
			}

		}
		inner(w, r, user)
	})
}

// cors responds to incoming requests and adds the appropriate cors headers
// TODO: corylanou: add the ability to configure this in our config
func cors(inner http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if origin := r.Header.Get("Origin"); origin != "" {
			w.Header().Set(`Access-Control-Allow-Origin`, origin)
			w.Header().Set(`Access-Control-Allow-Methods`, strings.Join([]string{
				`DELETE`,
				`GET`,
				`OPTIONS`,
				`POST`,
				`PUT`,
			}, ", "))

			w.Header().Set(`Access-Control-Allow-Headers`, strings.Join([]string{
				`Accept`,
				`Accept-Encoding`,
				`Authorization`,
				`Content-Length`,
				`Content-Type`,
				`X-CSRF-Token`,
				`X-HTTP-Method-Override`,
			}, ", "))

			w.Header().Set(`Access-Control-Expose-Headers`, strings.Join([]string{
				`Date`,
				`X-InfluxDB-Version`,
				`X-InfluxDB-Build`,
			}, ", "))
		}

		if r.Method == "OPTIONS" {
			return
		}

		inner.ServeHTTP(w, r)
	})
}

func requestID(inner http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// X-Request-Id takes priority.
		rid := r.Header.Get("X-Request-Id")

		// If X-Request-Id is empty, then check Request-Id
		if rid == "" {
			rid = r.Header.Get("Request-Id")
		}

		// If Request-Id is empty then generate a v1 UUID.
		if rid == "" {
			rid = uuid.TimeUUID().String()
		}

		// We read Request-Id in other handler code so we'll use that naming
		// convention from this point in the request cycle.
		r.Header.Set("Request-Id", rid)

		// Set the request ID on the response headers.
		// X-Request-Id is the most common name for a request ID header.
		w.Header().Set("X-Request-Id", rid)

		// We will also set Request-Id for backwards compatibility with previous
		// versions of InfluxDB.
		w.Header().Set("Request-Id", rid)

		inner.ServeHTTP(w, r)
	})
}

func (h *Handler) logging(inner http.Handler, name string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		l := &responseLogger{w: w}
		inner.ServeHTTP(l, r)
		h.CLFLogger.Println(buildLogLine(l, r, start))

		// Log server errors.
		if l.Status()/100 == 5 {
			errStr := l.Header().Get("X-InfluxDB-Error")
			if errStr != "" {
				h.Logger.Error(fmt.Sprintf("[%d] - %q", l.Status(), errStr))
			}
		}
	})
}

// if the env var is set, and the value is truthy, then we will *not*
// recover from a panic.
var willCrash bool

func init() {
	var err error
	if willCrash, err = strconv.ParseBool(os.Getenv(query.PanicCrashEnv)); err != nil {
		willCrash = false
	}
}

func (h *Handler) recovery(inner http.Handler, name string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		l := &responseLogger{w: w}

		defer func() {
			if err := recover(); err != nil {
				logLine := buildLogLine(l, r, start)
				logLine = fmt.Sprintf("%s [panic:%s] %s", logLine, err, debug.Stack())
				h.CLFLogger.Println(logLine)
				http.Error(w, http.StatusText(http.StatusInternalServerError), 500)
				atomic.AddInt64(&h.stats.RecoveredPanics, 1) // Capture the panic in _internal stats.

				if willCrash {
					h.CLFLogger.Println("\n\n=====\nAll goroutines now follow:")
					buf := debug.Stack()
					h.CLFLogger.Printf("%s\n", buf)
					os.Exit(1) // If we panic then the Go server will recover.
				}
			}
		}()

		inner.ServeHTTP(l, r)
	})
}

// Response represents a list of statement results.
type Response struct {
	Results []*query.Result
	Err     error
}

// MarshalJSON encodes a Response struct into JSON.
func (r Response) MarshalJSON() ([]byte, error) {
	// Define a struct that outputs "error" as a string.
	var o struct {
		Results []*query.Result `json:"results,omitempty"`
		Err     string          `json:"error,omitempty"`
	}

	// Copy fields to output struct.
	o.Results = r.Results
	if r.Err != nil {
		o.Err = r.Err.Error()
	}

	return json.Marshal(&o)
}

// UnmarshalJSON decodes the data into the Response struct.
func (r *Response) UnmarshalJSON(b []byte) error {
	var o struct {
		Results []*query.Result `json:"results,omitempty"`
		Err     string          `json:"error,omitempty"`
	}

	err := json.Unmarshal(b, &o)
	if err != nil {
		return err
	}
	r.Results = o.Results
	if o.Err != "" {
		r.Err = errors.New(o.Err)
	}
	return nil
}

// Error returns the first error from any statement.
// Returns nil if no errors occurred on any statements.
func (r *Response) Error() error {
	if r.Err != nil {
		return r.Err
	}
	for _, rr := range r.Results {
		if rr.Err != nil {
			return rr.Err
		}
	}
	return nil
}
