package plan

import (
	"go.opentelemetry.io/otel"
)

var Tracer = otel.Tracer("ExecPlan")
