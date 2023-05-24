package bootstrap

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBootstrap(t *testing.T) {
	n := 0
	exec := executor.NewMemExecutor(func(sql string) (executor.Result, error) {
		n++
		return executor.Result{}, nil
	})

	b := NewBootstrapper(
		&memLocker{},
		clock.NewHLCClock(func() int64 { return 0 }, 0),
		exec)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	require.NoError(t, b.Bootstrap(ctx))
	assert.Equal(t, len(initSQLs)+1, n)
}

func TestBootstrapAlreadyBootstrapped(t *testing.T) {
	n := 0
	exec := executor.NewMemExecutor(func(sql string) (executor.Result, error) {
		n++
		if sql == "show databases" {
			memRes := executor.NewMemResult(
				[]types.Type{types.New(types.T_varchar, 2, 0)},
				mpool.MustNewZero())
			memRes.NewBatch()
			executor.AppendStringRows(memRes, 0, []string{bootstrappedCheckerDB})
			return memRes.GetResult(), nil
		}
		return executor.Result{}, nil
	})

	b := NewBootstrapper(
		&memLocker{},
		clock.NewHLCClock(func() int64 { return 0 }, 0),
		exec)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	require.NoError(t, b.Bootstrap(ctx))
	assert.Equal(t, 1, n)
}

func TestBootstrapWithWait(t *testing.T) {
	n := 0
	exec := executor.NewMemExecutor(func(sql string) (executor.Result, error) {
		if sql == "show databases" && n == 1 {
			memRes := executor.NewMemResult(
				[]types.Type{types.New(types.T_varchar, 2, 0)},
				mpool.MustNewZero())
			memRes.NewBatch()
			executor.AppendStringRows(memRes, 0, []string{bootstrappedCheckerDB})
			return memRes.GetResult(), nil
		}
		n++
		return executor.Result{}, nil
	})

	b := NewBootstrapper(
		&memLocker{id: 1},
		clock.NewHLCClock(func() int64 { return 0 }, 0),
		exec)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	require.NoError(t, b.Bootstrap(ctx))
	assert.True(t, n > 0)
}

type memLocker struct {
	sync.Mutex
	id uint64
}

func (l *memLocker) Get(ctx context.Context) (bool, error) {
	l.Lock()
	defer l.Unlock()
	l.id++
	return l.id == 1, nil
}
