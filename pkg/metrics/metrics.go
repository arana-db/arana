package metrics

import "github.com/prometheus/client_golang/prometheus"

var (
	ParserDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "arana",
		Subsystem: "parser",
		Name:      "duration_seconds",
		Help:      "histogram of processing time (s) in parse SQL.",
		Buckets:   prometheus.ExponentialBuckets(0.00004, 2, 25), //40us ~ 11min
	})

	OptimizeDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "arana",
		Subsystem: "optimizer",
		Name:      "duration_seconds",
		Help:      "histogram of processing time (s) in optimizer.",
		Buckets:   prometheus.ExponentialBuckets(0.00004, 2, 25), //40us ~ 11min
	})

	ExecuteDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "arana",
		Subsystem: "executor",
		Name:      "duration_seconds",
		Help:      "histogram of processing time (s) in execute.",
		Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 30), //100us ~ 15h,
	})
)

func RegisterMetrics() {
	prometheus.MustRegister(ParserDuration)
	prometheus.MustRegister(OptimizeDuration)
	prometheus.MustRegister(ExecuteDuration)
}
