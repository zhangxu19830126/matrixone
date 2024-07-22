package embed

var (
	logConfig = `
service-type = "LOG"
data-dir = "%s"

[log]
level = "info"
format = "console"
max-size = 512
`

	tnConfig = `
service-type = "TN"
data-dir = "%s"

[log]
level = "info"
format = "console"
max-size = 512

[hakeeper-client]
service-addresses = [
  "127.0.0.1:32001",
]

[[fileservice]]
name = "LOCAL"
backend = "DISK"

[[fileservice]]
name = "SHARED"
backend = "DISK"
data-dir = "%s/shared"

[fileservice.cache]
memory-capacity = "32MB"
disk-capacity = "1GB"
disk-path = "%s/file-service-cache"

[[fileservice]]
name = "ETL"
backend = "DISK-ETL"

[tn]
uuid = "dn"
port-base = %d

[tn.Txn.Storage]
backend = "TAE"
log-backend = "logservice"

[tn.Ckp]
flush-interval = "60s"
min-count = 100
scan-interval = "5s"
incremental-interval = "180s"
global-min-count = 60

[tn.LogtailServer]
rpc-max-message-size = "16KiB"
rpc-payload-copy-buffer-size = "16KiB"
rpc-enable-checksum = true
logtail-collect-interval = "2ms"
logtail-response-send-timeout = "10s"
max-logtail-fetch-failure = 5
`

	cnConfig = `
service-type = "CN"
data-dir = "%s"

[log]
level = "info"
format = "console"
max-size = 512

[hakeeper-client]
service-addresses = [
	"127.0.0.1:32001",
]

[[fileservice]]
name = "LOCAL"
backend = "DISK"

[[fileservice]]
name = "SHARED"
backend = "DISK"
data-dir = "%s/shared"

[fileservice.cache]
memory-capacity = "32MB"
disk-capacity = "32MB"
disk-path = "%s/file-service-cache"

[[fileservice]]
name = "ETL"
backend = "DISK-ETL"

[cn]
uuid = "cn-%d"
port-base = %d

[cn.txn.trace]
dir = "trace%d"

[cn.Engine]
type = "distributed-tae"

[cn.frontend]
port = %d
unix-socket = "%s/mysql%d.sock"
`
)
