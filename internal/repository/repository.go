package repository

import (
	"context"
)

type Repository interface {
	Run(ctx context.Context)
}
