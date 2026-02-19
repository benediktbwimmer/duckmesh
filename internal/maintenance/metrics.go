package maintenance

import "github.com/prometheus/client_golang/prometheus"

var (
	compactionRunsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "duckmesh_compaction_runs_total",
			Help: "Total number of compaction runs by status.",
		},
		[]string{"status"},
	)
	compactionBytesRewritten = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "duckmesh_compaction_bytes_rewritten_total",
			Help: "Total source bytes rewritten by compaction runs.",
		},
	)
	gcFilesDeletedTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "duckmesh_gc_files_deleted_total",
			Help: "Total number of files deleted by retention/GC runs.",
		},
	)
	integrityRunsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "duckmesh_integrity_runs_total",
			Help: "Total number of integrity check runs by status.",
		},
		[]string{"status"},
	)
	integrityFilesCheckedTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "duckmesh_integrity_files_checked_total",
			Help: "Total number of unique files checked by integrity validation.",
		},
	)
	integrityMissingFilesTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "duckmesh_integrity_missing_files_total",
			Help: "Total number of missing files detected by integrity validation.",
		},
	)
	integritySizeMismatchFilesTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "duckmesh_integrity_size_mismatch_files_total",
			Help: "Total number of file size mismatches detected by integrity validation.",
		},
	)
)

func init() {
	prometheus.MustRegister(
		compactionRunsTotal,
		compactionBytesRewritten,
		gcFilesDeletedTotal,
		integrityRunsTotal,
		integrityFilesCheckedTotal,
		integrityMissingFilesTotal,
		integritySizeMismatchFilesTotal,
	)
}
