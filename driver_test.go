package dbstats

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"testing"
	"time"
)

var (
	hook                 *fakeHook
	fake                 *fakeDriver
	queryer              *fakeDriver
	execer               *fakeDriver
	execerQueryer        *fakeDriver
	stats                Driver
	queryerStats         Driver
	execerStats          Driver
	execerQueryerStats   Driver
	queryerCalled        bool
	execerCalled         bool
	useColumnConverter   bool
	columnCoverterCalled bool
)

func init() {
	hook = &fakeHook{}
	fake = &fakeDriver{}
	queryer = &fakeDriver{isQueryer: true}
	execer = &fakeDriver{isExecer: true}
	execerQueryer = &fakeDriver{isQueryer: true, isExecer: true}
	stats = New(fake.Open)
	queryerStats = New(queryer.Open)
	execerStats = New(execer.Open)
	execerQueryerStats = New(execerQueryer.Open)
	stats.AddHook(hook)
	queryerStats.AddHook(hook)
	execerStats.AddHook(hook)
	execerQueryerStats.AddHook(hook)
	sql.Register("fakeStats", stats)
	sql.Register("fakeQueryerStats", queryerStats)
	sql.Register("fakeExecerStats", execerStats)
	sql.Register("fakeExecerQueryerStats", execerQueryerStats)
}

func reset() {
	fake.openNames = nil
	queryerCalled = false
	execerCalled = false
	useColumnConverter = false
	columnCoverterCalled = false
	stats.Reset()
	hook.reset()
}

type fakeDriver struct {
	openNames []string
	isQueryer bool
	isExecer  bool
}

func (d *fakeDriver) Open(name string) (driver.Conn, error) {
	d.openNames = append(d.openNames, name)
	if d.isExecer && d.isQueryer {
		return &fakeExecerQueryer{}, nil
	} else if d.isQueryer {
		return &fakeQueryer{}, nil
	} else if d.isExecer {
		return &fakeExecer{}, nil
	}
	return &fakeConn{}, nil
}

type fakeConn struct{}

func (c *fakeConn) Prepare(query string) (driver.Stmt, error) {
	if useColumnConverter {
		return &fakeColumnCoverter{}, nil
	}
	return &fakeStmt{}, nil
}
func (c *fakeConn) Close() error {
	return nil
}
func (c *fakeConn) Begin() (driver.Tx, error) {
	return &fakeTx{}, nil
}

type fakeQueryer struct{ fakeConn }

func (q *fakeQueryer) Query(query string, args []driver.Value) (driver.Rows, error) {
	queryerCalled = true
	return &fakeRows{}, nil
}

type fakeExecer struct{ fakeConn }

func (e *fakeExecer) Exec(query string, args []driver.Value) (driver.Result, error) {
	execerCalled = true
	return &fakeResult{}, nil
}

type fakeExecerQueryer struct{ fakeConn }

func (q *fakeExecerQueryer) Query(query string, args []driver.Value) (driver.Rows, error) {
	queryerCalled = true
	return &fakeRows{}, nil
}
func (e *fakeExecerQueryer) Exec(query string, args []driver.Value) (driver.Result, error) {
	execerCalled = true
	return &fakeResult{}, nil
}

type fakeTx struct{}

func (t *fakeTx) Commit() error {
	return nil
}
func (t *fakeTx) Rollback() error {
	return nil
}

type fakeStmt struct{}

func (s *fakeStmt) Close() error {
	return nil
}
func (s *fakeStmt) NumInput() int {
	return 1
}
func (s *fakeStmt) Exec(args []driver.Value) (driver.Result, error) {
	return &fakeResult{}, nil
}
func (s *fakeStmt) Query(args []driver.Value) (driver.Rows, error) {
	return &fakeRows{}, nil
}

type passthroughValueConverter struct{}

func (vc passthroughValueConverter) ConvertValue(v interface{}) (driver.Value, error) {
	return driver.Value(v), nil
}

type fakeColumnCoverter struct{ fakeStmt }

func (vc *fakeColumnCoverter) ColumnConverter(idx int) driver.ValueConverter {
	columnCoverterCalled = true
	return passthroughValueConverter{}
}

type fakeRows struct {
	rows int
}

func (r *fakeRows) Columns() []string {
	return []string{"c0", "c1"}
}
func (r *fakeRows) Close() error {
	return nil
}
func (r *fakeRows) Next(dest []driver.Value) error {
	if r.rows > 0 {
		return io.EOF
	}
	dest[0] = int64(42)
	dest[1] = false
	r.rows++
	return nil
}

type fakeResult struct{}

func (r *fakeResult) LastInsertId() (int64, error) {
	return 1, nil
}
func (r *fakeResult) RowsAffected() (int64, error) {
	return 2, nil
}

type fakeHook struct {
	connOpenedCount   int
	connClosedCount   int
	stmtPreparedCount int
	stmtClosedCount   int
	txBeganCount      int
	txCommitedCount   int
	txRolledbackCount int
	queriedCount      int
	execedCount       int
	rowIteratedCount  int
}

func (h *fakeHook) reset() {
	h.connOpenedCount = 0
	h.connClosedCount = 0
	h.stmtPreparedCount = 0
	h.stmtClosedCount = 0
	h.txBeganCount = 0
	h.txCommitedCount = 0
	h.txRolledbackCount = 0
	h.queriedCount = 0
	h.execedCount = 0
	h.rowIteratedCount = 0
}

func (h *fakeHook) ConnOpened() {
	h.connOpenedCount++
}
func (h *fakeHook) ConnClosed() {
	h.connClosedCount++
}
func (h *fakeHook) StmtPrepared(query string) {
	h.stmtPreparedCount++
}
func (h *fakeHook) StmtClosed() {
	h.stmtClosedCount++
}
func (h *fakeHook) TxBegan() {
	h.txBeganCount++
}
func (h *fakeHook) TxCommitted() {
	h.txCommitedCount++
}
func (h *fakeHook) TxRolledback() {
	h.txRolledbackCount++
}
func (h *fakeHook) Queried(d time.Duration, query string) {
	h.queriedCount++
}
func (h *fakeHook) Execed(d time.Duration, query string) {
	h.execedCount++
}
func (h *fakeHook) RowIterated() {
	h.rowIteratedCount++
}

func TestDriverHandlerValueConverterCorrectly(t *testing.T) {
	reset()
	useColumnConverter = true
	db, _ := sql.Open("fakeStats", "")
	defer db.Close()

	stmt, _ := db.Prepare("SELECT c0, c1 FROM my_table WHERE myvar=?")
	defer stmt.Close()
	rows, err := stmt.Query(int64(1))
	if err != nil {
		t.Fatalf("Failed to Query: %v", err)
	}
	defer rows.Close()
	if !columnCoverterCalled {
		t.Errorf("expected ColumnConverter interface to be called")
	}
}

func TestDriverHandlesExecerQueryerCorrectly(t *testing.T) {
	reset()
	db, _ := sql.Open("fakeExecerQueryerStats", "")
	defer db.Close()
	db.Exec("UPDATE my_table SET myvar=?", 1)
	db.Query("SELECT c0, c1 FROM my_table WHERE myvar=?", 1)
	if !execerCalled {
		t.Errorf("Expected Execer interface to be called")
	}
	if !queryerCalled {
		t.Errorf("Expected Queryer interface to be called")
	}

	if hook.execedCount != 1 {
		t.Errorf("Expected Execed to be called 1 time, got %d", hook.execedCount)
	}
	if hook.queriedCount != 1 {
		t.Errorf("Expected Queried to be called 1 time, got %d", hook.queriedCount)
	}

	s, _ := db.Prepare("SELECT * FROM my_table WHERE id=?")
	s.Close()
	if execerQueryerStats.TotalStmts() != 1 {
		t.Errorf("Expected TotalStmts == 1, got %d", execerQueryerStats.TotalStmts())
	}
}

func TestDriverHandlesExecerCorrectly(t *testing.T) {
	reset()
	db, _ := sql.Open("fakeExecerStats", "")
	defer db.Close()

	_, err := db.Exec("UPDATE my_table SET myvar=?", 1)
	switch {
	case err != nil:
		t.Errorf("Exec returned error: %v", err)
	case !execerCalled:
		t.Errorf("Expected execer.Exec to be called")
	case execerStats.Execs() != 1:
		t.Errorf("Expected Execs == 1, got %d", execerStats.Execs())
	}

	if hook.execedCount != 1 {
		t.Errorf("Expected Execed to be called 1 time, got %d", hook.execedCount)
	}

}

func TestDriverHandlesQueryerCorrectly(t *testing.T) {
	reset()
	db, _ := sql.Open("fakeQueryerStats", "")
	defer db.Close()

	rows, err := db.Query("SELECT c0, c1 FROM my_table WHERE myvar=?", 1)
	switch {
	case err != nil:
		t.Errorf("Query returned error: %v", err)
	case !queryerCalled:
		t.Errorf("Expected Queryer.Query to be called")
	case queryerStats.Queries() != 1:
		t.Errorf("Expected Queries == 1, got %d", queryerStats.Queries())
	}
	rows.Close()

	if hook.queriedCount != 1 {
		t.Errorf("Expected Queried to be called 1 time, got %d", hook.queriedCount)
	}
}

func TestDriverKeepsTxStats(t *testing.T) {
	reset()
	db, _ := sql.Open("fakeStats", "")
	defer db.Close()
	tx, _ := db.Begin()
	tx2, _ := db.Begin()

	if stats.OpenTxs() != 2 {
		t.Errorf("expected OpenTxs to return 2, got %d", stats.OpenTxs())
	}

	tx2.Rollback()
	switch {
	case stats.OpenTxs() != 1:
		t.Errorf("Expected Rollback to close transaction")
	case stats.TotalTxs() != 2:
		t.Errorf("Expected TotalTxs to return 2 even after Rollback: got %d", stats.TotalTxs())
	case stats.RolledbackTxs() != 1:
		t.Errorf("Expected RolledbackTxs to be 1, got %d", stats.RolledbackTxs())
	}

	tx.Commit()
	switch {
	case stats.OpenTxs() != 0:
		t.Errorf("Expected Commit to close transaction")
	case stats.TotalTxs() != 2:
		t.Errorf("Expected TotalTxs to return 2 even after Commit: got %d", stats.TotalTxs())
	case stats.CommittedTxs() != 1:
		t.Errorf("Expected CommittedTxs to be 1, got %d", stats.CommittedTxs())
	}

	if hook.txBeganCount != 2 {
		t.Errorf("Expected TxBegan to be called 2 times, got %d", hook.txBeganCount)
	}
	if hook.txCommitedCount != 1 {
		t.Errorf("Expected TxCommitted to be called 1 time, got %d", hook.txCommitedCount)
	}
	if hook.txRolledbackCount != 1 {
		t.Errorf("Expected TxRolledback to be called 1 time, got %d", hook.txRolledbackCount)
	}
}

func TestDriverKeepsStmtStats(t *testing.T) {
	reset()
	db, _ := sql.Open("fakeStats", "")
	defer db.Close()
	stmt, err := db.Prepare("SELECT now()")
	switch {
	case err != nil:
		t.Errorf("failed to Prepare: %v", err)
	case stats.OpenStmts() != 1:
		t.Errorf("expect OpenStmts to be 1, got %d", stats.OpenStmts())
	case stats.TotalStmts() != 1:
		t.Errorf("expect TotalStmts to be 1, got %d", stats.TotalStmts())
	}

	stmt.Exec(1)
	if stats.Execs() != 1 {
		t.Errorf("expect stmt.Exec to cause Execs to increase")
	}
	rows, _ := stmt.Query(1)
	if stats.Queries() != 1 {
		t.Errorf("expect stmt.Query to cause Queries to increase")
	}
	rowCount := 0
	for rows.Next() {
		rowCount++
	}
	if stats.RowsIterated() != rowCount {
		t.Errorf("Expected RowsIterated == %d, got %d", rowCount, stats.RowsIterated())
	}
	rows.Close()

	stmt.Close()
	if stats.OpenStmts() != 0 {
		t.Errorf("Expected open statements to be zero after close: got %d", stats.OpenStmts())
	}

	if hook.stmtPreparedCount != 1 {
		t.Errorf("Expected StmtPrepared to be called 1 time, got %d", hook.stmtPreparedCount)
	}
	if hook.stmtClosedCount != 1 {
		t.Errorf("Expected StmtClosed to be called 1 time, got %d", hook.stmtClosedCount)
	}
	if hook.queriedCount != 1 {
		t.Errorf("Expected Queried to be called 1 time, got %d", hook.queriedCount)
	}
	if hook.execedCount != 1 {
		t.Errorf("Expected Execed to be called 1 time, got %d", hook.queriedCount)
	}
	if hook.rowIteratedCount != 1 {
		t.Errorf("Expected RowIteratred to be called 1 time, got %d", hook.queriedCount)
	}
}

func TestDriverFowardsToWrapped(t *testing.T) {
	reset()
	params := "my params"
	db, _ := sql.Open("fakeStats", params)
	defer db.Close()
	err := db.Ping()

	switch {
	case err != nil:
		t.Errorf("Ping returned error: %v", err)
	case len(fake.openNames) == 0:
		t.Errorf("Open request did not get forwarded to fakeDriver")
	case fake.openNames[0] != params:
		t.Errorf("Did not pass params correctly to fakeDriver: %q!=%q", fake.openNames[0], params)
	}
}

func TestDriverKeepsConnectionStats(t *testing.T) {
	reset()
	db, _ := sql.Open("fakeStats", "")
	db.SetMaxIdleConns(10)
	db.Ping()
	switch {
	case stats.TotalConns() != 1:
		t.Errorf("Expected there to be 1 total connection, actually was %d", stats.TotalConns())
	case stats.OpenConns() != 1:
		t.Errorf("Expected there to be 1 open connection, actually was %d", stats.OpenConns())
	}

	db.Close()
	if stats.OpenConns() != 0 {
		t.Errorf("Expected no open connects after db close, got %d", stats.OpenConns())
	}

	if hook.connOpenedCount != 1 {
		t.Errorf("Expected hook to have ConnOpened called 1 time, got %d", hook.connOpenedCount)
	}
	if hook.connClosedCount != 1 {
		t.Errorf("Expected hook to have ConnClosed called 1 time, got %d", hook.connClosedCount)
	}
}

type errDriver struct{}

func (d errDriver) Open(name string) (driver.Conn, error) {
	return nil, fmt.Errorf("failed to open")
}

func TestHandlerOpenReturnsErr(t *testing.T) {
	sql.Register("errorStats", New(errDriver{}.Open))
	db, _ := sql.Open("errorStats", "")
	err := db.Ping()
	if err == nil {
		t.Errorf("Expected error to be returned")
	}
}
