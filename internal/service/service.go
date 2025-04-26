package service

import (
	"context"
	"log"
	"worker-service/internal/repository"
	"worker-service/internal/worker"
)

type Service struct {
	repository repository.Repository
	worker     worker.Worker
}

func NewService(rep repository.Repository, wrk worker.Worker) *Service {
	return &Service{repository: rep, worker: wrk}
}

func (s *Service) Serve(ctx context.Context) {
	log.Println("serve repository ...")
	go func() {
		s.repository.Run(ctx)
	}()

	log.Println("serve worker ...")
	go func() {
		s.worker.Work(ctx)
	}()
}
