package dbstats

import (
	"sync/atomic"
	"time"
)

// CounterHook is a Hook that keeps counters of various stats with
// respect to database usage.
type CounterHook struct {
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

	connErrs    int64
	stmtErrs    int64
	txOpenErrs  int64
	txCloseErrs int64
	queryErrs   int64
	execErrs    int64
	rowErrs     int64
}

// OpenConns returns the current count of open connections.
func (h *CounterHook) OpenConns() int {
	return int(atomic.LoadInt64(&h.openConns))
}

// TotalConns returns the total number of connections ever made.
func (h *CounterHook) TotalConns() int {
	return int(atomic.LoadInt64(&h.totalConns))
}

// OpenStmts returns the current count of prepared statements.
func (h *CounterHook) OpenStmts() int {
	return int(atomic.LoadInt64(&h.openStmts))
}

// TotalStmts returns the total number of prepared statements ever made.
func (h *CounterHook) TotalStmts() int {
	return int(atomic.LoadInt64(&h.totalStmts))
}

// OpenTxs returns the current number of open transactions
func (h *CounterHook) OpenTxs() int {
	return int(atomic.LoadInt64(&h.openTxs))
}

// TotalTxs returns the total number of transactions ever openned.
func (h *CounterHook) TotalTxs() int {
	return int(atomic.LoadInt64(&h.totalTxs))
}

// CommittedTxs returns the total number of transactions that were committed.
func (h *CounterHook) CommittedTxs() int {
	return int(atomic.LoadInt64(&h.committedTxs))
}

// RolledbackTxs returns the total number of transactions there were rolled back.
func (h *CounterHook) RolledbackTxs() int {
	return int(atomic.LoadInt64(&h.rolledbackTxs))
}

// Queries returns the total number of Query statements ran.
func (h *CounterHook) Queries() int {
	return int(atomic.LoadInt64(&h.queries))
}

// Execs returns the total number of Exex statements ran.
func (h *CounterHook) Execs() int {
	return int(atomic.LoadInt64(&h.execs))
}

// RowsIterated returns the total number of rows that have been iterated through.
func (h *CounterHook) RowsIterated() int {
	return int(atomic.LoadInt64(&h.rowsIterated))
}

// ConnErrs returns the number of errors encountered trying to open a connection.
func (h *CounterHook) ConnErrs() int {
	return int(atomic.LoadInt64(&h.connErrs))
}

// StmtErrs returns the number of errors encountered trying to prepare a statement.
func (h *CounterHook) StmtErrs() int {
	return int(atomic.LoadInt64(&h.stmtErrs))
}

// TxOpenErrs returns the number of errors encountered trying to start a transaction.
func (h *CounterHook) TxOpenErrs() int {
	return int(atomic.LoadInt64(&h.txOpenErrs))
}

// TxCloseErrs returns the number of errors encountered trying to Commit or Rollback
// a transacttion.
func (h *CounterHook) TxCloseErrs() int {
	return int(atomic.LoadInt64(&h.txCloseErrs))
}

// QueryErrs returns the number of errors encountered trying to run a query.
func (h *CounterHook) QueryErrs() int {
	return int(atomic.LoadInt64(&h.queryErrs))
}

// ExexErrs returns the number of errors encountered trying to exec command.
func (h *CounterHook) ExecErrs() int {
	return int(atomic.LoadInt64(&h.execErrs))
}

// RowErrs returns the number of error encountered while iterating rows.
func (h *CounterHook) RowErrs() int {
	return int(atomic.LoadInt64(&h.rowErrs))
}

// ConnOpened implements ConnOpened of the Hook interface.
func (h *CounterHook) ConnOpened(err error) {
	if err == nil {
		atomic.AddInt64(&h.openConns, 1)
		atomic.AddInt64(&h.totalConns, 1)
	} else {
		atomic.AddInt64(&h.connErrs, 1)
	}
}

// ConnClosed implements ConnClosed of the Hook interface.
func (h *CounterHook) ConnClosed(err error) {
	atomic.AddInt64(&h.openConns, -1)
}

// StmtPrepared implements StmtPrepared of the Hook interface.
func (h *CounterHook) StmtPrepared(query string, err error) {
	if err == nil {
		atomic.AddInt64(&h.openStmts, 1)
		atomic.AddInt64(&h.totalStmts, 1)
	} else {
		atomic.AddInt64(&h.stmtErrs, 1)
	}
}

// StmtClosed implements StmtClosed of the Hook interface.
func (h *CounterHook) StmtClosed(err error) {
	atomic.AddInt64(&h.openStmts, -1)
}

// TxBegan implements TxBegan of the Hook interface.
func (h *CounterHook) TxBegan(err error) {
	if err == nil {
		atomic.AddInt64(&h.openTxs, 1)
		atomic.AddInt64(&h.totalTxs, 1)
	} else {
		atomic.AddInt64(&h.txOpenErrs, 1)
	}
}

// TxCommitted implements TxCommitted of the Hook interface.
func (h *CounterHook) TxCommitted(err error) {
	atomic.AddInt64(&h.openTxs, -1)
	if err == nil {
		atomic.AddInt64(&h.committedTxs, 1)
	} else {
		atomic.AddInt64(&h.txCloseErrs, 1)
	}
}

// TxRolledback implements TxRolledback of the Hook interface.
func (h *CounterHook) TxRolledback(err error) {
	atomic.AddInt64(&h.openTxs, -1)
	if err == nil {
		atomic.AddInt64(&h.rolledbackTxs, 1)
	} else {
		atomic.AddInt64(&h.txCloseErrs, 1)
	}
}

// Queried implements Queried of the Hook interface.
func (h *CounterHook) Queried(d time.Duration, query string, err error) {
	if err == nil {
		atomic.AddInt64(&h.queries, 1)
	} else {
		atomic.AddInt64(&h.queryErrs, 1)
	}
}

// Execed implements Execed of the Hook interface.
func (h *CounterHook) Execed(d time.Duration, query string, err error) {
	if err == nil {
		atomic.AddInt64(&h.execs, 1)
	} else {
		atomic.AddInt64(&h.execErrs, 1)
	}
}

// RowIterated implements RowIterated of the Hook interface.
func (h *CounterHook) RowIterated(err error) {
	if err == nil {
		atomic.AddInt64(&h.rowsIterated, 1)
	} else {
		atomic.AddInt64(&h.rowErrs, 1)
	}
}
