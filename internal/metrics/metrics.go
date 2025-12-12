package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// RaftState represents the current Raft state: 0=Follower, 1=Candidate, 2=Leader
	RaftState = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "aether_raft_state",
		Help: "Current Raft state (0=Follower, 1=Candidate, 2=Leader)",
	})

	// CommitIndex tracks the latest committed log index
	CommitIndex = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "aether_raft_commit_index",
		Help: "The latest committed Raft log index",
	})

	// ActiveConnections tracks the number of active PostgreSQL connections
	ActiveConnections = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "aether_active_connections",
		Help: "Number of active PostgreSQL client connections",
	})

	// SQLQueryCount counts SQL queries by type (read/write)
	SQLQueryCount = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "aether_sql_queries_total",
			Help: "Total number of SQL queries processed",
		},
		[]string{"type"}, // labels: read, write
	)
)

// Init registers all metrics with Prometheus
// Note: With promauto, metrics are automatically registered, but this function
// is provided for explicit initialization if needed
func Init() {
	// Metrics are already registered via promauto
	// This function can be used for any additional setup
}

// SetRaftState updates the Raft state metric
func SetRaftState(state int) {
	RaftState.Set(float64(state))
}

// SetCommitIndex updates the commit index metric
func SetCommitIndex(idx uint64) {
	CommitIndex.Set(float64(idx))
}

// IncConnection increments the active connections counter
func IncConnection() {
	ActiveConnections.Inc()
}

// DecConnection decrements the active connections counter
func DecConnection() {
	ActiveConnections.Dec()
}

// IncSQL increments the SQL query counter for the given operation type
func IncSQL(opType string) {
	SQLQueryCount.WithLabelValues(opType).Inc()
}
