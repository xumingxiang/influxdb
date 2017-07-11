package coordinator

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"time"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/monitor"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/tsdb"
)

// ErrDatabaseNameRequired is returned when executing statements that require a database,
// when a database has not been provided.
var ErrDatabaseNameRequired = errors.New("database name required")

type pointsWriter interface {
	WritePointsInto(*IntoWriteRequest) error
}

// StatementExecutor executes a statement in the query.
type StatementExecutor struct {
	MetaClient MetaClient

	// TaskManager holds the StatementExecutor that handles task-related commands.
	TaskManager influxql.StatementExecutor

	// TSDB storage for local node.
	TSDBStore TSDBStore

	// ShardMapper for mapping shards when executing a SELECT statement.
	ShardMapper ShardMapper

	// Holds monitoring data for SHOW STATS and SHOW DIAGNOSTICS.
	Monitor *monitor.Monitor

	// Used for rewriting points back into system for SELECT INTO statements.
	PointsWriter pointsWriter

	// Select statement limits
	MaxSelectPointN   int
	MaxSelectSeriesN  int
	MaxSelectBucketsN int
}

// ExecuteStatement executes the given statement with the given execution context.
func (e *StatementExecutor) ExecuteStatement(stmt influxql.Statement, ctx influxql.ExecutionContext) error {
	// Select statements are handled separately so that they can be streamed.
	if stmt, ok := stmt.(*influxql.SelectStatement); ok {
		return e.executeSelectStatement(stmt, &ctx)
	}

	switch stmt := stmt.(type) {
	case *influxql.AlterRetentionPolicyStatement:
		if err := e.executeAlterRetentionPolicyStatement(stmt); err != nil {
			return err
		}
		if ctx.ReadOnly {
			return ctx.Ok(influxql.ReadOnlyWarning(stmt.String()))
		}
		return ctx.Ok()
	case *influxql.CreateContinuousQueryStatement:
		if err := e.executeCreateContinuousQueryStatement(stmt); err != nil {
			return err
		}
		if ctx.ReadOnly {
			return ctx.Ok(influxql.ReadOnlyWarning(stmt.String()))
		}
		return ctx.Ok()
	case *influxql.CreateDatabaseStatement:
		if err := e.executeCreateDatabaseStatement(stmt); err != nil {
			return err
		}
		if ctx.ReadOnly {
			return ctx.Ok(influxql.ReadOnlyWarning(stmt.String()))
		}
		return ctx.Ok()
	case *influxql.CreateRetentionPolicyStatement:
		if err := e.executeCreateRetentionPolicyStatement(stmt); err != nil {
			return err
		}
		if ctx.ReadOnly {
			return ctx.Ok(influxql.ReadOnlyWarning(stmt.String()))
		}
		return ctx.Ok()
	case *influxql.CreateSubscriptionStatement:
		if err := e.executeCreateSubscriptionStatement(stmt); err != nil {
			return err
		}
		if ctx.ReadOnly {
			return ctx.Ok(influxql.ReadOnlyWarning(stmt.String()))
		}
		return ctx.Ok()
	case *influxql.CreateUserStatement:
		if err := e.executeCreateUserStatement(stmt); err != nil {
			return err
		}
		if ctx.ReadOnly {
			return ctx.Ok(influxql.ReadOnlyWarning(stmt.String()))
		}
		return ctx.Ok()
	case *influxql.DeleteSeriesStatement:
		if err := e.executeDeleteSeriesStatement(stmt, ctx.Database); err != nil {
			return err
		}
		if ctx.ReadOnly {
			return ctx.Ok(influxql.ReadOnlyWarning(stmt.String()))
		}
		return ctx.Ok()
	case *influxql.DropContinuousQueryStatement:
		if err := e.executeDropContinuousQueryStatement(stmt); err != nil {
			return err
		}
		if ctx.ReadOnly {
			return ctx.Ok(influxql.ReadOnlyWarning(stmt.String()))
		}
		return ctx.Ok()
	case *influxql.DropDatabaseStatement:
		if err := e.executeDropDatabaseStatement(stmt); err != nil {
			return err
		}
		if ctx.ReadOnly {
			return ctx.Ok(influxql.ReadOnlyWarning(stmt.String()))
		}
		return ctx.Ok()
	case *influxql.DropMeasurementStatement:
		if err := e.executeDropMeasurementStatement(stmt, ctx.Database); err != nil {
			return err
		}
		if ctx.ReadOnly {
			return ctx.Ok(influxql.ReadOnlyWarning(stmt.String()))
		}
		return ctx.Ok()
	case *influxql.DropSeriesStatement:
		if err := e.executeDropSeriesStatement(stmt, ctx.Database); err != nil {
			return err
		}
		if ctx.ReadOnly {
			return ctx.Ok(influxql.ReadOnlyWarning(stmt.String()))
		}
		return ctx.Ok()
	case *influxql.DropRetentionPolicyStatement:
		if err := e.executeDropRetentionPolicyStatement(stmt); err != nil {
			return err
		}
		if ctx.ReadOnly {
			return ctx.Ok(influxql.ReadOnlyWarning(stmt.String()))
		}
		return ctx.Ok()
	case *influxql.DropShardStatement:
		if err := e.executeDropShardStatement(stmt); err != nil {
			return err
		}
		if ctx.ReadOnly {
			return ctx.Ok(influxql.ReadOnlyWarning(stmt.String()))
		}
		return ctx.Ok()
	case *influxql.DropSubscriptionStatement:
		if err := e.executeDropSubscriptionStatement(stmt); err != nil {
			return err
		}
		if ctx.ReadOnly {
			return ctx.Ok(influxql.ReadOnlyWarning(stmt.String()))
		}
		return ctx.Ok()
	case *influxql.DropUserStatement:
		if err := e.executeDropUserStatement(stmt); err != nil {
			return err
		}
		if ctx.ReadOnly {
			return ctx.Ok(influxql.ReadOnlyWarning(stmt.String()))
		}
		return ctx.Ok()
	case *influxql.GrantStatement:
		if err := e.executeGrantStatement(stmt); err != nil {
			return err
		}
		if ctx.ReadOnly {
			return ctx.Ok(influxql.ReadOnlyWarning(stmt.String()))
		}
		return ctx.Ok()
	case *influxql.GrantAdminStatement:
		if err := e.executeGrantAdminStatement(stmt); err != nil {
			return err
		}
		if ctx.ReadOnly {
			return ctx.Ok(influxql.ReadOnlyWarning(stmt.String()))
		}
		return ctx.Ok()
	case *influxql.RevokeStatement:
		if err := e.executeRevokeStatement(stmt); err != nil {
			return err
		}
		if ctx.ReadOnly {
			return ctx.Ok(influxql.ReadOnlyWarning(stmt.String()))
		}
		return ctx.Ok()
	case *influxql.RevokeAdminStatement:
		if err := e.executeRevokeAdminStatement(stmt); err != nil {
			return err
		}
		if ctx.ReadOnly {
			return ctx.Ok(influxql.ReadOnlyWarning(stmt.String()))
		}
		return ctx.Ok()
	case *influxql.ShowContinuousQueriesStatement:
		return e.executeShowContinuousQueriesStatement(stmt, &ctx)
	case *influxql.ShowDatabasesStatement:
		return e.executeShowDatabasesStatement(stmt, &ctx)
	case *influxql.ShowDiagnosticsStatement:
		return e.executeShowDiagnosticsStatement(stmt, &ctx)
	case *influxql.ShowGrantsForUserStatement:
		return e.executeShowGrantsForUserStatement(stmt, &ctx)
	case *influxql.ShowMeasurementsStatement:
		return e.executeShowMeasurementsStatement(stmt, &ctx)
	case *influxql.ShowRetentionPoliciesStatement:
		return e.executeShowRetentionPoliciesStatement(stmt, &ctx)
	case *influxql.ShowShardsStatement:
		return e.executeShowShardsStatement(stmt, &ctx)
	case *influxql.ShowShardGroupsStatement:
		return e.executeShowShardGroupsStatement(stmt, &ctx)
	case *influxql.ShowStatsStatement:
		return e.executeShowStatsStatement(stmt, &ctx)
	case *influxql.ShowSubscriptionsStatement:
		return e.executeShowSubscriptionsStatement(stmt, &ctx)
	case *influxql.ShowTagValuesStatement:
		return e.executeShowTagValues(stmt, &ctx)
	case *influxql.ShowUsersStatement:
		return e.executeShowUsersStatement(stmt, &ctx)
	case *influxql.SetPasswordUserStatement:
		if err := e.executeSetPasswordUserStatement(stmt); err != nil {
			return err
		}
		if ctx.ReadOnly {
			return ctx.Ok(influxql.ReadOnlyWarning(stmt.String()))
		}
		return ctx.Ok()
	case *influxql.ShowQueriesStatement, *influxql.KillQueryStatement:
		// Send query related statements to the task manager.
		return e.TaskManager.ExecuteStatement(stmt, ctx)
	default:
		return influxql.ErrInvalidQuery
	}
}

func (e *StatementExecutor) executeAlterRetentionPolicyStatement(stmt *influxql.AlterRetentionPolicyStatement) error {
	rpu := &meta.RetentionPolicyUpdate{
		Duration:           stmt.Duration,
		ReplicaN:           stmt.Replication,
		ShardGroupDuration: stmt.ShardGroupDuration,
	}

	// Update the retention policy.
	if err := e.MetaClient.UpdateRetentionPolicy(stmt.Database, stmt.Name, rpu, stmt.Default); err != nil {
		return err
	}
	return nil
}

func (e *StatementExecutor) executeCreateContinuousQueryStatement(q *influxql.CreateContinuousQueryStatement) error {
	// Verify that retention policies exist.
	var err error
	verifyRPFn := func(n influxql.Node) {
		if err != nil {
			return
		}
		switch m := n.(type) {
		case *influxql.Measurement:
			var rp *meta.RetentionPolicyInfo
			if rp, err = e.MetaClient.RetentionPolicy(m.Database, m.RetentionPolicy); err != nil {
				return
			} else if rp == nil {
				err = fmt.Errorf("%s: %s.%s", meta.ErrRetentionPolicyNotFound, m.Database, m.RetentionPolicy)
			}
		default:
			return
		}
	}

	influxql.WalkFunc(q, verifyRPFn)

	if err != nil {
		return err
	}

	return e.MetaClient.CreateContinuousQuery(q.Database, q.Name, q.String())
}

func (e *StatementExecutor) executeCreateDatabaseStatement(stmt *influxql.CreateDatabaseStatement) error {
	if !meta.ValidName(stmt.Name) {
		// TODO This should probably be in `(*meta.Data).CreateDatabase`
		// but can't go there until 1.1 is used everywhere
		return meta.ErrInvalidName
	}

	if !stmt.RetentionPolicyCreate {
		_, err := e.MetaClient.CreateDatabase(stmt.Name)
		return err
	}

	// If we're doing, for example, CREATE DATABASE "db" WITH DURATION 1d then
	// the name will not yet be set. We only need to validate non-empty
	// retention policy names, such as in the statement:
	// 	CREATE DATABASE "db" WITH DURATION 1d NAME "xyz"
	if stmt.RetentionPolicyName != "" && !meta.ValidName(stmt.RetentionPolicyName) {
		return meta.ErrInvalidName
	}

	spec := meta.RetentionPolicySpec{
		Name:               stmt.RetentionPolicyName,
		Duration:           stmt.RetentionPolicyDuration,
		ReplicaN:           stmt.RetentionPolicyReplication,
		ShardGroupDuration: stmt.RetentionPolicyShardGroupDuration,
	}
	_, err := e.MetaClient.CreateDatabaseWithRetentionPolicy(stmt.Name, &spec)
	return err
}

func (e *StatementExecutor) executeCreateRetentionPolicyStatement(stmt *influxql.CreateRetentionPolicyStatement) error {
	if !meta.ValidName(stmt.Name) {
		// TODO This should probably be in `(*meta.Data).CreateRetentionPolicy`
		// but can't go there until 1.1 is used everywhere
		return meta.ErrInvalidName
	}

	spec := meta.RetentionPolicySpec{
		Name:               stmt.Name,
		Duration:           &stmt.Duration,
		ReplicaN:           &stmt.Replication,
		ShardGroupDuration: stmt.ShardGroupDuration,
	}

	// Create new retention policy.
	_, err := e.MetaClient.CreateRetentionPolicy(stmt.Database, &spec, stmt.Default)
	if err != nil {
		return err
	}

	return nil
}

func (e *StatementExecutor) executeCreateSubscriptionStatement(q *influxql.CreateSubscriptionStatement) error {
	return e.MetaClient.CreateSubscription(q.Database, q.RetentionPolicy, q.Name, q.Mode, q.Destinations)
}

func (e *StatementExecutor) executeCreateUserStatement(q *influxql.CreateUserStatement) error {
	_, err := e.MetaClient.CreateUser(q.Name, q.Password, q.Admin)
	return err
}

func (e *StatementExecutor) executeDeleteSeriesStatement(stmt *influxql.DeleteSeriesStatement, database string) error {
	if dbi := e.MetaClient.Database(database); dbi == nil {
		return influxql.ErrDatabaseNotFound(database)
	}

	// Convert "now()" to current time.
	stmt.Condition = influxql.Reduce(stmt.Condition, &influxql.NowValuer{Now: time.Now().UTC()})

	// Locally delete the series.
	return e.TSDBStore.DeleteSeries(database, stmt.Sources, stmt.Condition)
}

func (e *StatementExecutor) executeDropContinuousQueryStatement(q *influxql.DropContinuousQueryStatement) error {
	return e.MetaClient.DropContinuousQuery(q.Database, q.Name)
}

// executeDropDatabaseStatement drops a database from the cluster.
// It does not return an error if the database was not found on any of
// the nodes, or in the Meta store.
func (e *StatementExecutor) executeDropDatabaseStatement(stmt *influxql.DropDatabaseStatement) error {
	if e.MetaClient.Database(stmt.Name) == nil {
		return nil
	}

	// Locally delete the datababse.
	if err := e.TSDBStore.DeleteDatabase(stmt.Name); err != nil {
		return err
	}

	// Remove the database from the Meta Store.
	return e.MetaClient.DropDatabase(stmt.Name)
}

func (e *StatementExecutor) executeDropMeasurementStatement(stmt *influxql.DropMeasurementStatement, database string) error {
	if dbi := e.MetaClient.Database(database); dbi == nil {
		return influxql.ErrDatabaseNotFound(database)
	}

	// Locally drop the measurement
	return e.TSDBStore.DeleteMeasurement(database, stmt.Name)
}

func (e *StatementExecutor) executeDropSeriesStatement(stmt *influxql.DropSeriesStatement, database string) error {
	if dbi := e.MetaClient.Database(database); dbi == nil {
		return influxql.ErrDatabaseNotFound(database)
	}

	// Check for time in WHERE clause (not supported).
	if influxql.HasTimeExpr(stmt.Condition) {
		return errors.New("DROP SERIES doesn't support time in WHERE clause")
	}

	// Locally drop the series.
	return e.TSDBStore.DeleteSeries(database, stmt.Sources, stmt.Condition)
}

func (e *StatementExecutor) executeDropShardStatement(stmt *influxql.DropShardStatement) error {
	// Locally delete the shard.
	if err := e.TSDBStore.DeleteShard(stmt.ID); err != nil {
		return err
	}

	// Remove the shard reference from the Meta Store.
	return e.MetaClient.DropShard(stmt.ID)
}

func (e *StatementExecutor) executeDropRetentionPolicyStatement(stmt *influxql.DropRetentionPolicyStatement) error {
	dbi := e.MetaClient.Database(stmt.Database)
	if dbi == nil {
		return nil
	}

	if dbi.RetentionPolicy(stmt.Name) == nil {
		return nil
	}

	// Locally drop the retention policy.
	if err := e.TSDBStore.DeleteRetentionPolicy(stmt.Database, stmt.Name); err != nil {
		return err
	}

	return e.MetaClient.DropRetentionPolicy(stmt.Database, stmt.Name)
}

func (e *StatementExecutor) executeDropSubscriptionStatement(q *influxql.DropSubscriptionStatement) error {
	return e.MetaClient.DropSubscription(q.Database, q.RetentionPolicy, q.Name)
}

func (e *StatementExecutor) executeDropUserStatement(q *influxql.DropUserStatement) error {
	return e.MetaClient.DropUser(q.Name)
}

func (e *StatementExecutor) executeGrantStatement(stmt *influxql.GrantStatement) error {
	return e.MetaClient.SetPrivilege(stmt.User, stmt.On, stmt.Privilege)
}

func (e *StatementExecutor) executeGrantAdminStatement(stmt *influxql.GrantAdminStatement) error {
	return e.MetaClient.SetAdminPrivilege(stmt.User, true)
}

func (e *StatementExecutor) executeRevokeStatement(stmt *influxql.RevokeStatement) error {
	priv := influxql.NoPrivileges

	// Revoking all privileges means there's no need to look at existing user privileges.
	if stmt.Privilege != influxql.AllPrivileges {
		p, err := e.MetaClient.UserPrivilege(stmt.User, stmt.On)
		if err != nil {
			return err
		}
		// Bit clear (AND NOT) the user's privilege with the revoked privilege.
		priv = *p &^ stmt.Privilege
	}

	return e.MetaClient.SetPrivilege(stmt.User, stmt.On, priv)
}

func (e *StatementExecutor) executeRevokeAdminStatement(stmt *influxql.RevokeAdminStatement) error {
	return e.MetaClient.SetAdminPrivilege(stmt.User, false)
}

func (e *StatementExecutor) executeSetPasswordUserStatement(q *influxql.SetPasswordUserStatement) error {
	return e.MetaClient.UpdateUser(q.Name, q.Password)
}

func (e *StatementExecutor) executeSelectStatement(stmt *influxql.SelectStatement, ctx *influxql.ExecutionContext) error {
	if stmt.Target != nil && stmt.Target.Measurement.Database == "" {
		return errNoDatabaseInTarget
	}

	itrs, stmt, err := e.createIterators(stmt, ctx)
	if err != nil {
		return err
	}
	result, err := func() (*influxql.ResultSet, error) {
		if stmt.Target != nil && ctx.ReadOnly {
			return ctx.CreateResult(influxql.ReadOnlyWarning(stmt.String()))
		}
		return ctx.CreateResult()
	}()
	if err != nil {
		return err
	}

	// If we are writing the points, we need to send this result to the
	// goroutine that will be writing the points so the goroutine can emit the
	// number of points that have been written.
	if stmt.Target != nil {
		// Replace the result we just passed with our own created result. This
		// allows us to write to some location that is being read by the
		// writer.
		r := &influxql.ResultSet{
			ID:      ctx.StatementID,
			AbortCh: ctx.AbortCh,
		}
		r.Init()
		go e.writeResult(stmt, r, result)
		result = r
	}
	defer result.Close()

	// Set the columns of the result to the statement columns.
	columnNames := stmt.ColumnNames()
	columns := make([]influxql.Column, len(columnNames))
	offset := 0
	for i, name := range columnNames {
		columns[i].Name = name
		if i == 0 && name == "time" {
			columns[i].Type = influxql.Time
			offset++
			continue
		}

		switch itrs[i-offset].(type) {
		case influxql.FloatIterator:
			columns[i].Type = influxql.Float
		case influxql.IntegerIterator:
			columns[i].Type = influxql.Integer
		case influxql.StringIterator:
			columns[i].Type = influxql.String
		case influxql.BooleanIterator:
			columns[i].Type = influxql.Boolean
		}
	}
	result = result.WithColumns(columns...)

	// Generate a row emitter from the iterator set.
	em := influxql.NewEmitter(itrs, stmt.TimeAscending())
	defer em.Close()

	// Retrieve the time zone location. Default to using UTC.
	loc := time.UTC
	if stmt.Location != nil {
		loc = stmt.Location
	}

	var series *influxql.Series
	for {
		// Fill buffer. Close the series if no more points remain.
		t, name, tags, err := em.LoadBuf()
		if err != nil {
			// An error occurred while reading the iterators. If we are in the
			// middle of processing a series, assume the error comes from
			// reading the series. If it has come before we have created any
			// series, send the error to the result itself.
			if series != nil {
				series.Error(err)
				series.Close()
			} else {
				result.Error(err)
			}
			return influxql.ErrQueryCanceled
		} else if t == influxql.ZeroTime {
			if series != nil {
				series.Close()
			}
			return nil
		}

		// Read next set of values from all iterators at a given time/name/tags.
		values := make([]interface{}, len(columns))
		if stmt.OmitTime {
			em.ReadInto(t, name, tags, values)
		} else {
			values[0] = time.Unix(0, t).In(loc)
			em.ReadInto(t, name, tags, values[1:])
		}

		if series == nil {
			s, ok := result.CreateSeriesWithTags(name, tags)
			if !ok {
				return influxql.ErrQueryAborted
			}
			series = s
		} else if series.Name != name || !series.Tags.Equals(&tags) {
			series.Close()
			s, ok := result.CreateSeriesWithTags(name, tags)
			if !ok {
				return influxql.ErrQueryAborted
			}
			series = s
		}

		if ok := series.Emit(values); !ok {
			series.Close()
			return influxql.ErrQueryAborted
		}
	}
}

func (e *StatementExecutor) createIterators(stmt *influxql.SelectStatement, ctx *influxql.ExecutionContext) ([]influxql.Iterator, *influxql.SelectStatement, error) {
	// It is important to "stamp" this time so that everywhere we evaluate `now()` in the statement is EXACTLY the same `now`
	now := time.Now().UTC()
	opt := influxql.SelectOptions{
		InterruptCh: ctx.InterruptCh,
		NodeID:      ctx.ExecutionOptions.NodeID,
		MaxSeriesN:  e.MaxSelectSeriesN,
		Authorizer:  ctx.Authorizer,
	}

	// Replace instances of "now()" with the current time, and check the resultant times.
	nowValuer := influxql.NowValuer{Now: now, Location: stmt.Location}
	stmt = stmt.Reduce(&nowValuer)

	var err error
	opt.MinTime, opt.MaxTime, err = influxql.TimeRange(stmt.Condition, stmt.Location)
	if err != nil {
		return nil, stmt, err
	}

	if opt.MaxTime.IsZero() {
		opt.MaxTime = time.Unix(0, influxql.MaxTime)
	}
	if opt.MinTime.IsZero() {
		opt.MinTime = time.Unix(0, influxql.MinTime).UTC()
	}

	// Convert DISTINCT into a call.
	stmt.RewriteDistinct()

	// Remove "time" from fields list.
	stmt.RewriteTimeFields()

	// Rewrite time condition.
	if err := stmt.RewriteTimeCondition(now); err != nil {
		return nil, stmt, err
	}

	// Rewrite any regex conditions that could make use of the index.
	stmt.RewriteRegexConditions()

	// Create an iterator creator based on the shards in the cluster.
	ic, err := e.ShardMapper.MapShards(stmt.Sources, &opt)
	if err != nil {
		return nil, stmt, err
	}
	defer ic.Close()

	// Rewrite wildcards, if any exist.
	tmp, err := stmt.RewriteFields(ic)
	if err != nil {
		return nil, stmt, err
	}
	stmt = tmp

	if e.MaxSelectBucketsN > 0 && !stmt.IsRawQuery {
		interval, err := stmt.GroupByInterval()
		if err != nil {
			return nil, stmt, err
		}

		if interval > 0 {
			// Determine the start and end time matched to the interval (may not match the actual times).
			min := opt.MinTime.Truncate(interval)
			max := opt.MaxTime.Truncate(interval).Add(interval)

			// Determine the number of buckets by finding the time span and dividing by the interval.
			buckets := int64(max.Sub(min)) / int64(interval)
			if int(buckets) > e.MaxSelectBucketsN {
				return nil, stmt, fmt.Errorf("max-select-buckets limit exceeded: (%d/%d)", buckets, e.MaxSelectBucketsN)
			}
		}
	}

	// Create a set of iterators from a selection.
	itrs, err := influxql.Select(stmt, ic, &opt)
	if err != nil {
		return nil, stmt, err
	}

	if e.MaxSelectPointN > 0 {
		monitor := influxql.PointLimitMonitor(itrs, influxql.DefaultStatsInterval, e.MaxSelectPointN)
		ctx.Query.Monitor(monitor)
	}
	return itrs, stmt, nil
}

func (e *StatementExecutor) executeShowContinuousQueriesStatement(stmt *influxql.ShowContinuousQueriesStatement, ctx *influxql.ExecutionContext) error {
	dis := e.MetaClient.Databases()

	result, err := ctx.CreateResult()
	if err != nil {
		return err
	}
	defer result.Close()

	result = result.WithColumns(
		influxql.Column{Name: "name", Type: influxql.String},
		influxql.Column{Name: "query", Type: influxql.String},
	)
	for _, di := range dis {
		series, ok := result.CreateSeries(di.Name)
		if !ok {
			return influxql.ErrQueryAborted
		}
		for _, cqi := range di.ContinuousQueries {
			series.Emit([]interface{}{cqi.Name, cqi.Query})
		}
		series.Close()
	}
	return nil
}

func (e *StatementExecutor) executeShowDatabasesStatement(q *influxql.ShowDatabasesStatement, ctx *influxql.ExecutionContext) error {
	dis := e.MetaClient.Databases()
	a := ctx.ExecutionOptions.Authorizer

	result, err := ctx.CreateResult()
	if err != nil {
		return err
	}
	defer result.Close()

	result = result.WithColumns(influxql.Column{Name: "name"})
	series, ok := result.CreateSeries("databases")
	if !ok {
		return influxql.ErrQueryAborted
	}
	defer series.Close()

	for _, di := range dis {
		// Only include databases that the user is authorized to read or write.
		if a.AuthorizeDatabase(influxql.ReadPrivilege, di.Name) || a.AuthorizeDatabase(influxql.WritePrivilege, di.Name) {
			series.Emit([]interface{}{di.Name})
		}
	}
	return nil
}

func (e *StatementExecutor) executeShowDiagnosticsStatement(stmt *influxql.ShowDiagnosticsStatement, ctx *influxql.ExecutionContext) error {
	diags, err := e.Monitor.Diagnostics()
	if err != nil {
		return err
	}

	// Get a sorted list of diagnostics keys.
	sortedKeys := make([]string, 0, len(diags))
	for k := range diags {
		sortedKeys = append(sortedKeys, k)
	}
	sort.Strings(sortedKeys)

	result, err := ctx.CreateResult()
	if err != nil {
		return err
	}
	defer result.Close()

	for _, k := range sortedKeys {
		if stmt.Module != "" && k != stmt.Module {
			continue
		}

		series, ok := result.WithColumns(diags[k].Columns...).CreateSeries(k)
		if !ok {
			return influxql.ErrQueryAborted
		}

		for _, row := range diags[k].Rows {
			series.Emit(row)
		}
		series.Close()
	}
	return nil
}

func (e *StatementExecutor) executeShowGrantsForUserStatement(q *influxql.ShowGrantsForUserStatement, ctx *influxql.ExecutionContext) error {
	priv, err := e.MetaClient.UserPrivileges(q.Name)
	if err != nil {
		return err
	}

	result, err := ctx.CreateResult()
	if err != nil {
		return err
	}
	defer result.Close()

	result = result.WithColumns(
		influxql.Column{Name: "database", Type: influxql.String},
		influxql.Column{Name: "privilege", Type: influxql.String},
	)
	series, ok := result.CreateSeries("")
	if !ok {
		return influxql.ErrQueryAborted
	}
	defer series.Close()

	for d, p := range priv {
		series.Emit([]interface{}{d, p.String()})
	}
	return nil
}

func (e *StatementExecutor) executeShowMeasurementsStatement(q *influxql.ShowMeasurementsStatement, ctx *influxql.ExecutionContext) error {
	if q.Database == "" {
		return ErrDatabaseNameRequired
	}

	names, err := e.TSDBStore.MeasurementNames(q.Database, q.Condition)
	if err != nil {
		return err
	}

	if q.Offset > 0 {
		if q.Offset >= len(names) {
			names = nil
		} else {
			names = names[q.Offset:]
		}
	}

	if q.Limit > 0 {
		if q.Limit < len(names) {
			names = names[:q.Limit]
		}
	}

	if len(names) == 0 {
		return ctx.Ok()
	}

	result, err := ctx.CreateResult()
	if err != nil {
		return err
	}
	defer result.Close()

	result = result.WithColumns(influxql.Column{Name: "name"})
	series, ok := result.CreateSeries("measurements")
	if !ok {
		return influxql.ErrQueryAborted
	}
	defer series.Close()

	for _, name := range names {
		series.Emit([]interface{}{string(name)})
	}
	return nil
}

func (e *StatementExecutor) executeShowRetentionPoliciesStatement(q *influxql.ShowRetentionPoliciesStatement, ctx *influxql.ExecutionContext) error {
	if q.Database == "" {
		return ErrDatabaseNameRequired
	}

	di := e.MetaClient.Database(q.Database)
	if di == nil {
		return influxdb.ErrDatabaseNotFound(q.Database)
	}

	result, err := ctx.CreateResult()
	if err != nil {
		return err
	}
	defer result.Close()

	result = result.WithColumns(
		influxql.Column{Name: "name", Type: influxql.String},
		influxql.Column{Name: "duration", Type: influxql.String},
		influxql.Column{Name: "shardGroupDuration", Type: influxql.String},
		influxql.Column{Name: "replicaN", Type: influxql.Integer},
		influxql.Column{Name: "default", Type: influxql.Boolean},
	)
	series, ok := result.CreateSeries("")
	if !ok {
		return influxql.ErrQueryAborted
	}
	defer series.Close()

	for _, rpi := range di.RetentionPolicies {
		series.Emit([]interface{}{rpi.Name, rpi.Duration.String(), rpi.ShardGroupDuration.String(), rpi.ReplicaN, di.DefaultRetentionPolicy == rpi.Name})
	}
	return nil
}

func (e *StatementExecutor) executeShowShardsStatement(stmt *influxql.ShowShardsStatement, ctx *influxql.ExecutionContext) error {
	dis := e.MetaClient.Databases()

	result, err := ctx.CreateResult()
	if err != nil {
		return err
	}
	defer result.Close()

	result = result.WithColumns(
		influxql.Column{Name: "id", Type: influxql.Integer},
		influxql.Column{Name: "database", Type: influxql.String},
		influxql.Column{Name: "retention_policy", Type: influxql.String},
		influxql.Column{Name: "shard_group", Type: influxql.Integer},
		influxql.Column{Name: "start_time", Type: influxql.String},
		influxql.Column{Name: "end_time", Type: influxql.String},
		influxql.Column{Name: "expiry_time", Type: influxql.String},
		influxql.Column{Name: "owners", Type: influxql.String},
	)
	for _, di := range dis {
		series, ok := result.CreateSeries(di.Name)
		if !ok {
			return influxql.ErrQueryAborted
		}

		for _, rpi := range di.RetentionPolicies {
			for _, sgi := range rpi.ShardGroups {
				// Shards associated with deleted shard groups are effectively deleted.
				// Don't list them.
				if sgi.Deleted() {
					continue
				}

				for _, si := range sgi.Shards {
					ownerIDs := make([]uint64, len(si.Owners))
					for i, owner := range si.Owners {
						ownerIDs[i] = owner.NodeID
					}

					series.Emit([]interface{}{
						si.ID,
						di.Name,
						rpi.Name,
						sgi.ID,
						sgi.StartTime.UTC().Format(time.RFC3339),
						sgi.EndTime.UTC().Format(time.RFC3339),
						sgi.EndTime.Add(rpi.Duration).UTC().Format(time.RFC3339),
						joinUint64(ownerIDs),
					})
				}
			}
		}
		series.Close()
	}
	return nil
}

func (e *StatementExecutor) executeShowShardGroupsStatement(stmt *influxql.ShowShardGroupsStatement, ctx *influxql.ExecutionContext) error {
	dis := e.MetaClient.Databases()

	result, err := ctx.CreateResult()
	if err != nil {
		return err
	}
	defer result.Close()

	result = result.WithColumns(
		influxql.Column{Name: "id", Type: influxql.Integer},
		influxql.Column{Name: "database", Type: influxql.String},
		influxql.Column{Name: "retention_policy", Type: influxql.String},
		influxql.Column{Name: "start_time", Type: influxql.String},
		influxql.Column{Name: "end_time", Type: influxql.String},
		influxql.Column{Name: "expiry_time", Type: influxql.String},
	)
	series, ok := result.CreateSeries("shard groups")
	if !ok {
		return influxql.ErrQueryAborted
	}
	defer series.Close()

	for _, di := range dis {
		for _, rpi := range di.RetentionPolicies {
			for _, sgi := range rpi.ShardGroups {
				// Shards associated with deleted shard groups are effectively deleted.
				// Don't list them.
				if sgi.Deleted() {
					continue
				}

				series.Emit([]interface{}{
					sgi.ID,
					di.Name,
					rpi.Name,
					sgi.StartTime.UTC().Format(time.RFC3339),
					sgi.EndTime.UTC().Format(time.RFC3339),
					sgi.EndTime.Add(rpi.Duration).UTC().Format(time.RFC3339),
				})
			}
		}
	}
	return nil
}

func (e *StatementExecutor) executeShowStatsStatement(stmt *influxql.ShowStatsStatement, ctx *influxql.ExecutionContext) error {
	stats, err := e.Monitor.Statistics(nil)
	if err != nil {
		return err
	}

	result, err := ctx.CreateResult()
	if err != nil {
		return err
	}
	defer result.Close()

	for _, stat := range stats {
		if stmt.Module != "" && stat.Name != stmt.Module {
			continue
		}

		result := result.WithColumns(stat.Columns()...)
		series, ok := result.CreateSeriesWithTags(stat.Name, influxql.NewTags(stat.Tags))
		if !ok {
			return influxql.ErrQueryAborted
		}

		row := make([]interface{}, 0, len(series.Columns))
		for _, k := range series.Columns {
			row = append(row, stat.Values[k.Name])
		}
		series.Emit(row)
		series.Close()
	}
	return nil
}

func (e *StatementExecutor) executeShowSubscriptionsStatement(stmt *influxql.ShowSubscriptionsStatement, ctx *influxql.ExecutionContext) error {
	dis := e.MetaClient.Databases()

	result, err := ctx.CreateResult()
	if err != nil {
		return err
	}
	defer result.Close()

	result = result.WithColumns(
		influxql.Column{Name: "retention_policy", Type: influxql.String},
		influxql.Column{Name: "name", Type: influxql.String},
		influxql.Column{Name: "mode", Type: influxql.String},
		influxql.Column{Name: "destinations", Type: influxql.String},
	)
	for _, di := range dis {
		var series *influxql.Series
		for _, rpi := range di.RetentionPolicies {
			for _, si := range rpi.Subscriptions {
				// Lazily initialize the series so we don't emit a series that
				// has no subscriptions.
				if series == nil {
					s, ok := result.CreateSeries(di.Name)
					if !ok {
						return influxql.ErrQueryAborted
					}
					series = s
				}
				series.Emit([]interface{}{rpi.Name, si.Name, si.Mode, si.Destinations})
			}
		}

		if series != nil {
			series.Close()
		}
	}
	return nil
}

func (e *StatementExecutor) executeShowTagValues(q *influxql.ShowTagValuesStatement, ctx *influxql.ExecutionContext) error {
	if q.Database == "" {
		return ErrDatabaseNameRequired
	}

	tagValues, err := e.TSDBStore.TagValues(q.Database, q.Condition)
	if err != nil {
		return err
	}

	result, err := ctx.CreateResult()
	if err != nil {
		return err
	}
	defer result.Close()

	result = result.WithColumns(
		influxql.Column{Name: "key", Type: influxql.String},
		influxql.Column{Name: "value", Type: influxql.String},
	)
	for _, m := range tagValues {
		values := m.Values

		if q.Offset > 0 {
			if q.Offset >= len(values) {
				values = nil
			} else {
				values = values[q.Offset:]
			}
		}

		if q.Limit > 0 {
			if q.Limit < len(values) {
				values = values[:q.Limit]
			}
		}

		if len(values) == 0 {
			continue
		}

		series, ok := result.CreateSeries(m.Measurement)
		if !ok {
			return influxql.ErrQueryAborted
		}
		for _, v := range values {
			series.Emit([]interface{}{v.Key, v.Value})
		}
		series.Close()
	}
	return nil
}

func (e *StatementExecutor) executeShowUsersStatement(q *influxql.ShowUsersStatement, ctx *influxql.ExecutionContext) error {
	result, err := ctx.CreateResult()
	if err != nil {
		return err
	}
	defer result.Close()

	result = result.WithColumns(
		influxql.Column{Name: "user", Type: influxql.String},
		influxql.Column{Name: "admin", Type: influxql.Boolean},
	)
	series, ok := result.CreateSeries("")
	if !ok {
		return influxql.ErrQueryAborted
	}
	defer series.Close()

	for _, ui := range e.MetaClient.Users() {
		series.Emit([]interface{}{ui.Name, ui.Admin})
	}
	return nil
}

// BufferedPointsWriter adds buffering to a pointsWriter so that SELECT INTO queries
// write their points to the destination in batches.
type BufferedPointsWriter struct {
	w               pointsWriter
	buf             []models.Point
	database        string
	retentionPolicy string
}

func (e *StatementExecutor) writeResult(stmt *influxql.SelectStatement, in, out *influxql.ResultSet) {
	defer out.Close()
	measurementName := stmt.Target.Measurement.Name

	var writeN int64
	points := make([]models.Point, 0, 10000)
	for series := range in.SeriesCh() {
		if series.Err != nil {
			continue
		}

		// Convert the tags from the influxql format to the one expected by models.
		name := measurementName
		if name == "" {
			name = series.Name
		}
		tags := models.NewTags(series.Tags.KeyValues())
		for row := range series.RowCh() {
			if row.Err != nil {
				continue
			}

			// Convert the row back to a point.
			point, err := convertRowToPoint(name, tags, series.Columns, row.Values)
			if err != nil {
				out.Error(err)
				return
			}
			points = append(points, point)

			if len(points) == cap(points) {
				if err := e.PointsWriter.WritePointsInto(&IntoWriteRequest{
					Database:        stmt.Target.Measurement.Database,
					RetentionPolicy: stmt.Target.Measurement.RetentionPolicy,
					Points:          points,
				}); err != nil {
					out.Error(err)
					return
				}
				writeN += int64(len(points))
				points = points[:0]
			}
		}
	}

	if len(points) > 0 {
		if err := e.PointsWriter.WritePointsInto(&IntoWriteRequest{
			Database:        stmt.Target.Measurement.Database,
			RetentionPolicy: stmt.Target.Measurement.RetentionPolicy,
			Points:          points,
		}); err != nil {
			out.Error(err)
			return
		}
		writeN += int64(len(points))
	}

	series, ok := out.WithColumns(
		influxql.Column{Name: "time", Type: influxql.Time},
		influxql.Column{Name: "written", Type: influxql.Integer},
	).CreateSeries("result")
	if !ok {
		return
	}
	series.Emit([]interface{}{time.Unix(0, 0).UTC(), writeN})
	series.Close()
}

var errNoDatabaseInTarget = errors.New("no database in target")

// convertRowToPoints will convert a query result Row into Points that can be written back in.
func convertRowToPoint(measurementName string, tags models.Tags, columns []influxql.Column, row []interface{}) (models.Point, error) {
	// Iterate through the columns and treat the first "time" field as the time.
	var t time.Time
	vals := make(map[string]interface{}, len(columns)-1)
	for i, c := range columns {
		if c.Type == influxql.Time && t.IsZero() {
			t = row[i].(time.Time)
		} else if val := row[i]; val != nil {
			vals[c.Name] = val
		}
	}

	// If the time is zero, there was no time for some reason.
	if t.IsZero() {
		return nil, errors.New("error finding time index in result")
	}
	return models.NewPoint(measurementName, tags, vals, t)
}

// NormalizeStatement adds a default database and policy to the measurements in statement.
func (e *StatementExecutor) NormalizeStatement(stmt influxql.Statement, defaultDatabase string) (err error) {
	influxql.WalkFunc(stmt, func(node influxql.Node) {
		if err != nil {
			return
		}
		switch node := node.(type) {
		case *influxql.ShowRetentionPoliciesStatement:
			if node.Database == "" {
				node.Database = defaultDatabase
			}
		case *influxql.ShowMeasurementsStatement:
			if node.Database == "" {
				node.Database = defaultDatabase
			}
		case *influxql.ShowTagValuesStatement:
			if node.Database == "" {
				node.Database = defaultDatabase
			}
		case *influxql.Measurement:
			switch stmt.(type) {
			case *influxql.DropSeriesStatement, *influxql.DeleteSeriesStatement:
			// DB and RP not supported by these statements so don't rewrite into invalid
			// statements
			default:
				err = e.normalizeMeasurement(node, defaultDatabase)
			}
		}
	})
	return
}

func (e *StatementExecutor) normalizeMeasurement(m *influxql.Measurement, defaultDatabase string) error {
	// Targets (measurements in an INTO clause) can have blank names, which means it will be
	// the same as the measurement name it came from in the FROM clause.
	if !m.IsTarget && m.Name == "" && m.Regex == nil {
		return errors.New("invalid measurement")
	}

	// Measurement does not have an explicit database? Insert default.
	if m.Database == "" {
		m.Database = defaultDatabase
	}

	// The database must now be specified by this point.
	if m.Database == "" {
		return ErrDatabaseNameRequired
	}

	// Find database.
	di := e.MetaClient.Database(m.Database)
	if di == nil {
		return influxdb.ErrDatabaseNotFound(m.Database)
	}

	// If no retention policy was specified, use the default.
	if m.RetentionPolicy == "" {
		if di.DefaultRetentionPolicy == "" {
			return fmt.Errorf("default retention policy not set for: %s", di.Name)
		}
		m.RetentionPolicy = di.DefaultRetentionPolicy
	}

	return nil
}

// IntoWriteRequest is a partial copy of cluster.WriteRequest
type IntoWriteRequest struct {
	Database        string
	RetentionPolicy string
	Points          []models.Point
}

// TSDBStore is an interface for accessing the time series data store.
type TSDBStore interface {
	CreateShard(database, policy string, shardID uint64, enabled bool) error
	WriteToShard(shardID uint64, points []models.Point) error

	RestoreShard(id uint64, r io.Reader) error
	BackupShard(id uint64, since time.Time, w io.Writer) error

	DeleteDatabase(name string) error
	DeleteMeasurement(database, name string) error
	DeleteRetentionPolicy(database, name string) error
	DeleteSeries(database string, sources []influxql.Source, condition influxql.Expr) error
	DeleteShard(id uint64) error

	MeasurementNames(database string, cond influxql.Expr) ([][]byte, error)
	TagValues(database string, cond influxql.Expr) ([]tsdb.TagValues, error)
}

var _ TSDBStore = LocalTSDBStore{}

// LocalTSDBStore embeds a tsdb.Store and implements IteratorCreator
// to satisfy the TSDBStore interface.
type LocalTSDBStore struct {
	*tsdb.Store
}

// ShardIteratorCreator is an interface for creating an IteratorCreator to access a specific shard.
type ShardIteratorCreator interface {
	ShardIteratorCreator(id uint64) influxql.IteratorCreator
}

// joinUint64 returns a comma-delimited string of uint64 numbers.
func joinUint64(a []uint64) string {
	var buf bytes.Buffer
	for i, x := range a {
		buf.WriteString(strconv.FormatUint(x, 10))
		if i < len(a)-1 {
			buf.WriteRune(',')
		}
	}
	return buf.String()
}
