package metrics
import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)
var (
	RaftState = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "aether_raft_state",
		Help: "Current Raft state (0=Follower, 1=Candidate, 2=Leader)",
	})
	CommitIndex = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "aether_raft_commit_index",
		Help: "The latest committed Raft log index",
	})
	ActiveConnections = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "aether_active_connections",
		Help: "Number of active PostgreSQL client connections",
	})
	SQLQueryCount = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "aether_sql_queries_total",
			Help: "Total number of SQL queries processed",
		},
		[]string{"type"},  
	)
)
func Init() {
}
func SetRaftState(state int) {
	RaftState.Set(float64(state))
}
func SetCommitIndex(idx uint64) {
	CommitIndex.Set(float64(idx))
}
func IncConnection() {
	ActiveConnections.Inc()
}
func DecConnection() {
	ActiveConnections.Dec()
}
func IncSQL(opType string) {
	SQLQueryCount.WithLabelValues(opType).Inc()
}
