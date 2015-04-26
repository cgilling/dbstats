package dbstats

import (
	"errors"
	"testing"
	"time"
)

var anErr = errors.New("random error")

func TestCounterHookConnections(t *testing.T) {
	h := &CounterHook{}

	h.ConnOpened(nil)
	if h.OpenConns() != 1 {
		t.Errorf("Expected ConnOpened to increment OpenConns to 1, got %d", h.OpenConns())
	}
	if h.TotalConns() != 1 {
		t.Errorf("Expected ConnOpened to increment TotalConns to 1, got %d", h.TotalConns())
	}

	h.ConnClosed(nil)
	if h.OpenConns() != 0 {
		t.Errorf("Expected ConnClosed to decrement OpenConns to 0, got %d", h.OpenConns())
	}
	if h.TotalConns() != 1 {
		t.Errorf("Expected ConnClosed to leave TotatlConns at 1, got %d", h.TotalConns())
	}

	h.ConnOpened(anErr)
	if h.OpenConns() > 0 {
		t.Errorf("Expected connection error to not increment open connection count")
	}
	if h.TotalConns() > 1 {
		t.Errorf("Expected connection error to not increment total connection count")
	}
	if h.ConnErrs() != 1 {
		t.Errorf("Expected connection error to increment ConnErrs")
	}
}

func TestCounterHookStatements(t *testing.T) {
	h := &CounterHook{}

	h.StmtPrepared("SELECT 1", nil)
	if h.OpenStmts() != 1 {
		t.Errorf("Expected StmtPrepared to increment OpenStmts to 1, got %d", h.OpenStmts())
	}
	if h.TotalStmts() != 1 {
		t.Errorf("Expected StmtPrepared to increment TotalStmts to 1, got %d", h.TotalStmts())
	}

	h.StmtClosed(nil)
	if h.OpenStmts() != 0 {
		t.Errorf("Expected StmtClosed to decrement OpenStmts to 0, got %d", h.OpenStmts())
	}
	if h.TotalStmts() != 1 {
		t.Errorf("Expected StmtClosed to leave TotalStmts at 1, got %d", h.TotalStmts())
	}

	h.StmtPrepared("SELECT 1", anErr)
	if h.OpenStmts() > 0 {
		t.Errorf("Expected statement open error to not increment OpenStmts")
	}
	if h.TotalStmts() > 1 {
		t.Errorf("Expected statement open error to not increment TotalStmts")
	}
	if h.StmtErrs() != 1 {
		t.Errorf("Expected statemet open error to increment StmtErrs")
	}
}

func TestCounterHookTransactions(t *testing.T) {
	h := &CounterHook{}

	h.TxBegan(nil)
	if h.OpenTxs() != 1 {
		t.Errorf("Expected TxBegan to increment OpenTxs to 1, got %d", h.OpenTxs())
	}
	if h.TotalTxs() != 1 {
		t.Errorf("Expected TxBegan to increment TotalTxs to 1, got %d", h.TotalTxs())
	}

	h.TxCommitted(nil)
	if h.OpenTxs() != 0 {
		t.Errorf("Expected TxCommitted to decrement OpenTxs to 0, got %d", h.OpenTxs())
	}
	if h.TotalTxs() != 1 {
		t.Errorf("Expected TxCommitted to leave TotalTxs at 1, got %d", h.TotalTxs())
	}
	if h.CommittedTxs() != 1 {
		t.Errorf("Expected TxCommitted to increment CommittedTxs to 1, got %d", h.CommittedTxs())
	}

	h.TxBegan(nil)
	h.TxRolledback(nil)
	if h.OpenTxs() != 0 {
		t.Errorf("Expected TxRolledback to decrement OpenTxs to 0, got %d", h.OpenTxs())
	}
	if h.TotalTxs() != 2 {
		t.Errorf("Expected TxRolledback to leave TotalTxs at 2, got %d", h.TotalTxs())
	}
	if h.RolledbackTxs() != 1 {
		t.Errorf("Expected TxRolledback to increment RolledbackTxs to 1, got %d", h.RolledbackTxs())
	}

	h.TxBegan(anErr)
	if h.OpenTxs() > 0 {
		t.Errorf("Expected error beginning transaction not to increment OpenTxs")
	}
	if h.TotalTxs() > 2 {
		t.Errorf("Expected error beginning transaction not to increment TotalTxs")
	}
	if h.TxOpenErrs() != 1 {
		t.Errorf("Expected error beginning transaction  to increment TxOpenErrs")
	}

	h.TxBegan(nil)
	h.TxCommitted(anErr)
	if h.CommittedTxs() > 1 {
		t.Errorf("Expected error committing transaction not to increment CommittedTxs")
	}
	if h.TxCloseErrs() != 1 {
		t.Errorf("Expected error committing transaction to increment TxCloseErrs")
	}

	h.TxBegan(nil)
	h.TxRolledback(anErr)
	if h.RolledbackTxs() > 1 {
		t.Errorf("Expected error rolling back transaction not to increment RolledbackTxs")
	}
	if h.TxCloseErrs() != 2 {
		t.Errorf("Expected error rolling back transaction to increment TxCloseErrs")
	}
}

func TestCounterHookQueriesExecsRows(t *testing.T) {
	h := &CounterHook{}

	h.Queried(time.Millisecond*10, "SELECT 1", nil)
	if h.Queries() != 1 {
		t.Errorf("Expected Queried to increment Queries to 1, got %d", h.Queries())
	}
	h.Queried(time.Millisecond*10, "SELECT 1", anErr)
	if h.Queries() > 1 {
		t.Errorf("Expected error on Query to not increment Queries")
	}
	if h.QueryErrs() != 1 {
		t.Errorf("Expected error on Query to increment QueryErrs")
	}

	h.Execed(time.Millisecond, "UPDATE my_table SET myvar=?", nil)
	if h.Execs() != 1 {
		t.Errorf("Expected Execed to increment Execs to 1, got %d", h.Execs())
	}
	h.Execed(time.Millisecond, "UPDATE my_table SET myvar=?", anErr)
	if h.Execs() > 1 {
		t.Errorf("Expected error on Exec to not increment Execs")
	}
	if h.ExecErrs() != 1 {
		t.Errorf("Expected error on Exec to increment ExecErrs")
	}

	h.RowIterated(nil)
	if h.RowsIterated() != 1 {
		t.Errorf("Expected RowIterated to increment RowsIterated to 1, got %d", h.RowsIterated())
	}
	h.RowIterated(anErr)
	if h.RowsIterated() > 1 {
		t.Errorf("Expected error on iterating row to not increment RowsIterated")
	}
	if h.RowErrs() != 1 {
		t.Errorf("Expected error on iterating row to increment RowErrs")
	}
}
