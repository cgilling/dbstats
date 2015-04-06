package dbstats

import (
	"database/sql/driver"
	"sync/atomic"
	"time"
)

// OpenFunc is the func used on driver.Driver. This is used as some driver libraries
// (lib/pq for example) do not expose their driver.Driver struct, but do expose an Open
// function.
type OpenFunc func(name string) (driver.Conn, error)

// Hook is an interface through which database events can be received.
type Hook interface {
	ConnOpened()
	ConnClosed()
	StmtPrepared(query string)
	StmtClosed()
	TxBegan()
	TxCommitted()
	TxRolledback()
	Queried(d time.Duration, query string)
	Execed(d time.Duration, query string)
	RowIterated()
}

type Driver interface {
	driver.Driver

	// OpenConns returns the current count of open connections.
	OpenConns() int

	// TotalConns returns the total number of connections ever made.
	TotalConns() int

	// OpenStmts returns the current count of prepared statements.
	OpenStmts() int

	// TotalStmts returns the total number of prepared statements ever made.
	TotalStmts() int

	// OpenTxs returns the current number of open transactions
	OpenTxs() int

	// TotalTxs returns the total number of transactions ever openned.
	TotalTxs() int

	// CommittedTxs returns the total number of transactions that were committed.
	CommittedTxs() int

	// RolledbackTxs returns the total number of transactions there were rolled back.
	RolledbackTxs() int

	// Queries returns the total number of Query statements ran.
	Queries() int

	// Execs returns the total number of Exex statements ran.
	Execs() int

	// RowsIterated returns the total number of rows that have been iterated through.
	RowsIterated() int

	// Reset resets all stats to default values.
	Reset()
}

func New(open OpenFunc) Driver {
	return &statsDriver{open: open}
}

type statsDriver struct {
	open          OpenFunc
	openConns     int64
	totalConns    int64
	openStmts     int64
	totalStmts    int64
	openTxs       int64
	totalTxs      int64
	committedTxs  int64
	rolledbackTxs int64
	queries       int64
	execs         int64
	rowsIterated  int64
}

func (s *statsDriver) Open(name string) (driver.Conn, error) {
	c, err := s.open(name)
	if err != nil {
		return c, err
	}

	s.ConnOpened()
	statc := &statsConn{d: s, wrapped: c}
	q, isQ := c.(driver.Queryer)
	e, isE := c.(driver.Execer)
	if isE && isQ {
		return &statsExecerQueryer{
			statsConn:    statc,
			statsQueryer: &statsQueryer{statsConn: statc, wrapped: q},
			statsExecer:  &statsExecer{statsConn: statc, wrapped: e},
		}, nil
	} else if isQ {
		return &statsQueryer{statsConn: statc, wrapped: q}, nil
	} else if isE {
		return &statsExecer{statsConn: statc, wrapped: e}, nil
	}
	return statc, nil
}

func (s *statsDriver) Reset() {
	atomic.StoreInt64(&s.openConns, 0)
	atomic.StoreInt64(&s.totalConns, 0)
	atomic.StoreInt64(&s.openStmts, 0)
	atomic.StoreInt64(&s.totalStmts, 0)
	atomic.StoreInt64(&s.openTxs, 0)
	atomic.StoreInt64(&s.totalTxs, 0)
	atomic.StoreInt64(&s.committedTxs, 0)
	atomic.StoreInt64(&s.rolledbackTxs, 0)
	atomic.StoreInt64(&s.queries, 0)
	atomic.StoreInt64(&s.execs, 0)
	atomic.StoreInt64(&s.rowsIterated, 0)
}

func (s *statsDriver) OpenConns() int {
	return int(atomic.LoadInt64(&s.openConns))
}
func (s *statsDriver) TotalConns() int {
	return int(atomic.LoadInt64(&s.totalConns))
}
func (s *statsDriver) OpenStmts() int {
	return int(atomic.LoadInt64(&s.openStmts))
}
func (s *statsDriver) TotalStmts() int {
	return int(atomic.LoadInt64(&s.totalStmts))
}
func (s *statsDriver) OpenTxs() int {
	return int(atomic.LoadInt64(&s.openTxs))
}
func (s *statsDriver) TotalTxs() int {
	return int(atomic.LoadInt64(&s.totalTxs))
}
func (s *statsDriver) CommittedTxs() int {
	return int(atomic.LoadInt64(&s.committedTxs))
}
func (s *statsDriver) RolledbackTxs() int {
	return int(atomic.LoadInt64(&s.rolledbackTxs))
}
func (s *statsDriver) Queries() int {
	return int(atomic.LoadInt64(&s.queries))
}
func (s *statsDriver) Execs() int {
	return int(atomic.LoadInt64(&s.execs))
}
func (s *statsDriver) RowsIterated() int {
	return int(atomic.LoadInt64(&s.rowsIterated))
}

func (s *statsDriver) ConnOpened() {
	atomic.AddInt64(&s.openConns, 1)
	atomic.AddInt64(&s.totalConns, 1)
}
func (s *statsDriver) ConnClosed() {
	atomic.AddInt64(&s.openConns, -1)
}
func (s *statsDriver) StmtPrepared(query string) {
	atomic.AddInt64(&s.openStmts, 1)
	atomic.AddInt64(&s.totalStmts, 1)
}
func (s *statsDriver) StmtClosed() {
	atomic.AddInt64(&s.openStmts, -1)
}
func (s *statsDriver) TxBegan() {
	atomic.AddInt64(&s.openTxs, 1)
	atomic.AddInt64(&s.totalTxs, 1)
}
func (s *statsDriver) TxCommitted() {
	atomic.AddInt64(&s.openTxs, -1)
	atomic.AddInt64(&s.committedTxs, 1)
}
func (s *statsDriver) TxRolledback() {
	atomic.AddInt64(&s.openTxs, -1)
	atomic.AddInt64(&s.rolledbackTxs, 1)
}
func (s *statsDriver) Queried(d time.Duration, query string) {
	atomic.AddInt64(&s.queries, 1)
}
func (s *statsDriver) Execed(d time.Duration, query string) {
	atomic.AddInt64(&s.execs, 1)
}
func (s *statsDriver) RowIterated() {
	atomic.AddInt64(&s.rowsIterated, 1)
}

type statsConn struct {
	d       *statsDriver // the driver in which to store stats
	wrapped driver.Conn  // the wrapped connection
}

func (c *statsConn) Prepare(query string) (driver.Stmt, error) {
	s, err := c.wrapped.Prepare(query)
	if err == nil {
		c.d.StmtPrepared(query)
		cc, isCc := s.(driver.ColumnConverter)
		if isCc {
			s = &statsColumnConverter{
				statsStmt: &statsStmt{d: c.d, wrapped: s, query: query},
				wrapped:   cc,
			}
		} else {
			s = &statsStmt{d: c.d, wrapped: s, query: query}
		}
	}
	return s, err
}

func (c *statsConn) Close() error {
	err := c.wrapped.Close()
	c.d.ConnClosed()
	return err
}

func (c *statsConn) Begin() (driver.Tx, error) {
	tx, err := c.wrapped.Begin()
	if err == nil {
		c.d.TxBegan()
		tx = &statsTx{d: c.d, wrapped: tx}
	}
	return tx, err
}

type statsQueryer struct {
	*statsConn
	wrapped driver.Queryer
}

func (q *statsQueryer) Query(query string, args []driver.Value) (driver.Rows, error) {
	start := time.Now()
	r, err := q.wrapped.Query(query, args)
	dur := time.Now().Sub(start)
	if err == nil {
		q.statsConn.d.Queried(dur, query)
		r = &statsRows{d: q.statsConn.d, wrapped: r}
	}
	return r, err
}

type statsExecer struct {
	*statsConn
	wrapped driver.Execer
}

func (e *statsExecer) Exec(query string, args []driver.Value) (driver.Result, error) {
	start := time.Now()
	r, err := e.wrapped.Exec(query, args)
	dur := time.Now().Sub(start)
	if err == nil {
		e.statsConn.d.Execed(dur, query)
	}
	return r, err
}

type statsExecerQueryer struct {
	*statsConn
	*statsQueryer
	*statsExecer
}

type statsStmt struct {
	d       *statsDriver
	wrapped driver.Stmt
	query   string
}

type statsColumnConverter struct {
	*statsStmt
	wrapped driver.ColumnConverter
}

func (vc *statsColumnConverter) ColumnConverter(idx int) driver.ValueConverter {
	return vc.wrapped.ColumnConverter(idx)
}

func (s *statsStmt) Close() error {
	err := s.wrapped.Close()
	s.d.StmtClosed()
	return err
}

func (s *statsStmt) NumInput() int {
	return s.wrapped.NumInput()
}

func (s *statsStmt) Exec(args []driver.Value) (driver.Result, error) {
	start := time.Now()
	r, err := s.wrapped.Exec(args)
	dur := time.Now().Sub(start)
	if err == nil {
		s.d.Execed(dur, s.query)
	}
	return r, err
}

func (s *statsStmt) Query(args []driver.Value) (driver.Rows, error) {
	start := time.Now()
	r, err := s.wrapped.Query(args)
	dur := time.Now().Sub(start)
	if err == nil {
		s.d.Queried(dur, s.query)
		r = &statsRows{d: s.d, wrapped: r}
	}
	return r, err
}

type statsRows struct {
	d       *statsDriver
	wrapped driver.Rows
}

func (r *statsRows) Columns() []string {
	return r.wrapped.Columns()
}
func (r *statsRows) Close() error {
	return r.wrapped.Close()
}
func (r *statsRows) Next(dest []driver.Value) error {
	err := r.wrapped.Next(dest)
	if err == nil {
		r.d.RowIterated()
	}
	return err
}

type statsTx struct {
	d       *statsDriver
	wrapped driver.Tx
}

func (t *statsTx) Commit() error {
	err := t.wrapped.Commit()
	t.d.TxCommitted()
	return err
}

func (t *statsTx) Rollback() error {
	err := t.wrapped.Rollback()
	t.d.TxRolledback()
	return err
}
