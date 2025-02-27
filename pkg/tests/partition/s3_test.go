package partition

import (
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

// create table t1 (a primary, c int, b vecf32(2))
// insert into t1 select '1' from generate_series(1,10)g, '[1,0,0,0]'
func TestS3Insert(t *testing.T) {
	creates := []string{
		// "create table %s (c int) partition by hash(c) partitions 2",
		"create table %s(a int primary key,b vecf32(4), c varchar(10) ) partition by hash(a) partitions 2",
	}
	inserts := []string{
		// "insert into %s values(1)",
		"insert into %s select result, '[1,0,0,0]' , '1' from generate_series(1,2000000)g;",
	}

	runPartitionClusterTest(
		t,
		func(c embed.Cluster) {
			cn, err := c.GetCNService(0)
			require.NoError(t, err)

			db := testutils.GetDatabaseName(t)
			testutils.CreateTestDatabase(t, db, cn)

			for idx := range creates {
				table := fmt.Sprintf("%s_%d", t.Name(), idx)
				create := fmt.Sprintf(creates[idx], table)
				insert := fmt.Sprintf(inserts[idx], table)

				testutils.ExecSQL(
					t,
					db,
					cn,
					create,
				)

				fn := func() int64 {
					n := int64(0)
					for i := 0; i < 2; i++ {
						testutils.ExecSQLWithReadResult(
							t,
							db,
							cn,
							func(i int, s string, r executor.Result) {
								r.ReadRows(
									func(rows int, cols []*vector.Vector) bool {
										n += executor.GetFixedRows[int64](cols[0])[0]
										return true
									},
								)
							},
							fmt.Sprintf("select count(1) from %s_p%d", table, i),
						)
					}
					return n
				}

				testutils.ExecSQLWithReadResult(
					t,
					db,
					cn,
					func(i int, s string, r executor.Result) {
						require.Equal(t, uint64(2000000), r.AffectedRows)
					},
					insert,
				)
				require.Equal(t, int64(2000000), fn())

				// testutils.ExecSQLWithReadResult(
				// 	t,
				// 	db,
				// 	cn,
				// 	func(i int, s string, r executor.Result) {
				// 		r.ReadRows(
				// 			func(rows int, cols []*vector.Vector) bool {
				// 				require.Equal(t, int64(1), executor.GetFixedRows[int64](cols[0])[0])
				// 				return true
				// 			},
				// 		)
				// 	},
				// 	fmt.Sprintf("select count(1) from %s", table),
				// )

				// testutils.ExecSQL(
				// 	t,
				// 	db,
				// 	cn,
				// 	delete,
				// )
				// require.Equal(t, int64(0), fn())

				// testutils.ExecSQLWithReadResult(
				// 	t,
				// 	db,
				// 	cn,
				// 	func(i int, s string, r executor.Result) {
				// 		r.ReadRows(
				// 			func(rows int, cols []*vector.Vector) bool {
				// 				require.Equal(t, int64(0), executor.GetFixedRows[int64](cols[0])[0])
				// 				return true
				// 			},
				// 		)
				// 	},
				// 	fmt.Sprintf("select count(1) from %s", table),
				// )
			}
		},
	)
}
