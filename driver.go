package dbstats

import (
	"database/sql/driver"
	"io"
	"time"
)

// OpenFunc is the func used on driver.Driver. This is used as some driver libraries
// (lib/pq for example) do not expose their driver.Driver struct, but do expose an Open
// function.
type OpenFunc func(name string) (driver.Conn, error)

// Hook is an interface through which database events can be received. A Hook may received
// multiple events concurrently. Each function's last argument is of type error which will
// contain an error encountered while trying to perform the action. The one exception is
// RowInterated which will not return io.EOF because it is an expected return value.
type Hook interface {
	ConnOpened(err error)
	ConnClosed(err error)
	StmtPrepared(query string, err error)
	StmtClosed(err error)
	TxBegan(err error)
	TxCommitted(err error)
	TxRolledback(err error)
	Queried(d time.Duration, query string, err error)
	Execed(d time.Duration, query string, err error)
	RowIterated(err error)
}

type Driver interface {
	driver.Driver

	// AddHook will add a Hook to be called when various database events occurs. AddHook
	// should be called before any database activity happens as there is no gaurantee that
	// locking will occur between addining and using Hooks.
	AddHook(h Hook)
}

func New(open OpenFunc) Driver {
	return &statsDriver{open: open}
}

type statsDriver struct {
	open  OpenFunc
	hooks []Hook
}

func (s *statsDriver) Open(name string) (driver.Conn, error) {
	c, err := s.open(name)
	s.ConnOpened(err)
	if err != nil {
		return c, err
	}
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

func (s *statsDriver) AddHook(h Hook) {
	s.hooks = append(s.hooks, h)
}
func (s *statsDriver) ConnOpened(err error) {
	for _, h := range s.hooks {
		h.ConnOpened(err)
	}
}
func (s *statsDriver) ConnClosed(err error) {
	for _, h := range s.hooks {
		h.ConnClosed(err)
	}
}
func (s *statsDriver) StmtPrepared(query string, err error) {
	for _, h := range s.hooks {
		h.StmtPrepared(query, err)
	}
}
func (s *statsDriver) StmtClosed(err error) {
	for _, h := range s.hooks {
		h.StmtClosed(err)
	}
}
func (s *statsDriver) TxBegan(err error) {
	for _, h := range s.hooks {
		h.TxBegan(err)
	}
}
func (s *statsDriver) TxCommitted(err error) {
	for _, h := range s.hooks {
		h.TxCommitted(err)
	}
}
func (s *statsDriver) TxRolledback(err error) {
	for _, h := range s.hooks {
		h.TxRolledback(err)
	}
}
func (s *statsDriver) Queried(d time.Duration, query string, err error) {
	for _, h := range s.hooks {
		h.Queried(d, query, err)
	}
}
func (s *statsDriver) Execed(d time.Duration, query string, err error) {
	for _, h := range s.hooks {
		h.Execed(d, query, err)
	}
}
func (s *statsDriver) RowIterated(err error) {
	for _, h := range s.hooks {
		h.RowIterated(err)
	}
}

type statsConn struct {
	d       *statsDriver // the driver in which to store stats
	wrapped driver.Conn  // the wrapped connection
}

func (c *statsConn) Prepare(query string) (driver.Stmt, error) {
	s, err := c.wrapped.Prepare(query)
	c.d.StmtPrepared(query, err)
	if err == nil {
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
	c.d.ConnClosed(err)
	return err
}

func (c *statsConn) Begin() (driver.Tx, error) {
	tx, err := c.wrapped.Begin()
	c.d.TxBegan(err)
	if err == nil {
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
	q.statsConn.d.Queried(dur, query, err)
	if err == nil {
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
	e.statsConn.d.Execed(dur, query, err)
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
	s.d.StmtClosed(err)
	return err
}

func (s *statsStmt) NumInput() int {
	return s.wrapped.NumInput()
}

func (s *statsStmt) Exec(args []driver.Value) (driver.Result, error) {
	start := time.Now()
	r, err := s.wrapped.Exec(args)
	dur := time.Now().Sub(start)
	s.d.Execed(dur, s.query, err)
	return r, err
}

func (s *statsStmt) Query(args []driver.Value) (driver.Rows, error) {
	start := time.Now()
	r, err := s.wrapped.Query(args)
	dur := time.Now().Sub(start)
	s.d.Queried(dur, s.query, err)
	if err == nil {
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
	if err != io.EOF {
		r.d.RowIterated(err)
	}
	return err
}

type statsTx struct {
	d       *statsDriver
	wrapped driver.Tx
}

func (t *statsTx) Commit() error {
	err := t.wrapped.Commit()
	t.d.TxCommitted(err)
	return err
}

func (t *statsTx) Rollback() error {
	err := t.wrapped.Rollback()
	t.d.TxRolledback(err)
	return err
}
