package worker

import (
	"context"
)

type Worker interface {
	Work(ctx context.Context)
}
