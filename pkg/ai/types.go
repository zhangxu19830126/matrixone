package ai

type MCPServer interface {
	Start() error
	Stop() error
}
