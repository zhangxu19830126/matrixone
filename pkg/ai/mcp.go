package ai

import (
	"context"
	"strings"
	"time"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

type mcpServer struct {
	s    *server.MCPServer
	sse  *server.SSEServer
	exec executor.SQLExecutor
}

func NewMCPServer(
	exec executor.SQLExecutor,
) MCPServer {
	s := server.NewMCPServer(
		"MO",
		"1.0.0",
		server.WithToolCapabilities(false),
		server.WithRecovery(),
	)
	return &mcpServer{
		s:    s,
		sse:  server.NewSSEServer(s),
		exec: exec,
	}
}

func (m *mcpServer) Start() error {
	m.addDataBaseListTool()
	return m.sse.Start(":8080")
}

func (m *mcpServer) Stop() error {
	return m.sse.Shutdown(context.Background())
}

func (m *mcpServer) addDataBaseListTool() {
	t := mcp.NewTool("database-list",
		mcp.WithDescription("Perform list database operations"),
	)

	m.s.AddTool(
		t,
		func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			ctx, cancel := context.WithTimeout(ctx, time.Second*10)
			defer cancel()

			res, err := m.exec.Exec(
				ctx,
				"show databases",
				executor.Options{},
			)
			if err != nil {
				return mcp.NewToolResultError(err.Error()), nil
			}

			var databases []string
			res.ReadRows(func(rows int, cols []*vector.Vector) bool {
				databases = append(databases, executor.GetStringRows(cols[0])...)
				return true
			})

			return mcp.NewToolResultText(strings.Join(databases, ",")), nil
		},
	)
}
