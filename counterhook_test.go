package dbstats

import (
	"testing"
	"time"
)

func TestCounterHookConnections(t *testing.T) {
	h := &CounterHook{}

	h.ConnOpened()
	if h.OpenConns() != 1 {
		t.Errorf("Expected ConnOpened to increment OpenConns to 1, got %d", h.OpenConns())
	}
	if h.TotalConns() != 1 {
		t.Errorf("Expected ConnOpened to increment TotalConns to 1, got %d", h.TotalConns())
	}

	h.ConnClosed()
	if h.OpenConns() != 0 {
		t.Errorf("Expected ConnClosed to decrement OpenConns to 0, got %d", h.OpenConns())
	}
	if h.TotalConns() != 1 {
		t.Errorf("Expected ConnClosed to leave TotatlConns at 1, got %d", h.TotalConns())
	}
}

func TestCounterHookStatements(t *testing.T) {
	h := &CounterHook{}

	h.StmtPrepared("SELECT 1")
	if h.OpenStmts() != 1 {
		t.Errorf("Expected StmtPrepared to increment OpenStmts to 1, got %d", h.OpenStmts())
	}
	if h.TotalStmts() != 1 {
		t.Errorf("Expected StmtPrepared to increment TotalStmts to 1, got %d", h.TotalStmts())
	}

	h.StmtClosed()
	if h.OpenStmts() != 0 {
		t.Errorf("Expected StmtClosed to decrement OpenStmts to 0, got %d", h.OpenStmts())
	}
	if h.TotalStmts() != 1 {
		t.Errorf("Expected StmtClosed to leave TotalStmts at 1, got %d", h.TotalStmts())
	}
}

func TestCounterHookTransactions(t *testing.T) {
	h := &CounterHook{}

	h.TxBegan()
	if h.OpenTxs() != 1 {
		t.Errorf("Expected TxBegan to increment OpenTxs to 1, got %d", h.OpenTxs())
	}
	if h.TotalTxs() != 1 {
		t.Errorf("Expected TxBegan to increment TotalTxs to 1, got %d", h.TotalTxs())
	}

	h.TxCommitted()
	if h.OpenTxs() != 0 {
		t.Errorf("Expected TxCommitted to decrement OpenTxs to 0, got %d", h.OpenTxs())
	}
	if h.TotalTxs() != 1 {
		t.Errorf("Expected TxCommitted to leave TotalTxs at 1, got %d", h.TotalTxs())
	}
	if h.CommittedTxs() != 1 {
		t.Errorf("Expected TxCommitted to increment CommittedTxs to 1, got %d", h.CommittedTxs())
	}

	h.TxBegan()
	h.TxRolledback()
	if h.OpenTxs() != 0 {
		t.Errorf("Expected TxRolledback to decrement OpenTxs to 0, got %d", h.OpenTxs())
	}
	if h.TotalTxs() != 2 {
		t.Errorf("Expected TxRolledback to leave TotalTxs at 2, got %d", h.TotalTxs())
	}
	if h.RolledbackTxs() != 1 {
		t.Errorf("Expected TxRolledback to increment RolledbackTxs to 1, got %d", h.RolledbackTxs())
	}
}

func TestCounterHookQueriesExecsRows(t *testing.T) {
	h := &CounterHook{}

	h.Queried(time.Millisecond*10, "SELECT 1")
	if h.Queries() != 1 {
		t.Errorf("Expected Queried to increment Queries to 1, got %d", h.Queries())
	}

	h.Execed(time.Millisecond, "UPDATE my_table SET myvar=?")
	if h.Execs() != 1 {
		t.Errorf("Expected Execed to increment Execs to 1, got %d", h.Execs())
	}

	h.RowIterated()
	if h.RowsIterated() != 1 {
		t.Errorf("Expected RowIterated to increment RowsIterated to 1, got %d", h.RowsIterated())
	}
}
