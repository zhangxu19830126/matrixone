package op

import (
	"sync"
)

var (
	mu              sync.Mutex
	activeProcesses map[string]map[string]string
	activeTxnSQL    map[string]string
)

func AddSQL(txnID, sql string) {
	mu.Lock()
	defer mu.Unlock()
	if activeTxnSQL == nil {
		activeTxnSQL = make(map[string]string)
	}

	activeTxnSQL[txnID] = sql
}

func EndSQL(txnID string) {
	mu.Lock()
	defer mu.Unlock()
	if activeTxnSQL == nil {
		activeTxnSQL = make(map[string]string)
	}

	delete(activeTxnSQL, txnID)
}

func UpdateActive(txnID, process, info string) {
	if txnID == "" {
		return
	}
	mu.Lock()
	defer mu.Unlock()
	if activeProcesses == nil {
		activeProcesses = make(map[string]map[string]string)
	}

	if _, ok := activeProcesses[txnID]; !ok {
		activeProcesses[txnID] = make(map[string]string)
	}

	if info != "" {
		activeProcesses[txnID][process] = info
	} else {
		delete(activeProcesses[txnID], process)
	}
}

func GetActive(txnID string) string {
	mu.Lock()
	defer mu.Unlock()
	v := "["
	if values, ok := activeProcesses[txnID]; ok {
		for p, op := range values {
			v += p + ":" + op + ","
		}
	}
	v += "], last sql: " + activeTxnSQL[txnID]
	return v
}
