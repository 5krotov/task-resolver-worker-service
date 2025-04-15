package worker_service

import (
	"net/http"
)

// WorkerService implements service.Service but doesn't necessarily expose an HTTP endpoint.
// You could add a monitoring endpoint, for example, or just keep it empty.
type WorkerService struct {
	handler *WorkerHandler
}

func NewWorkerService(handler *WorkerHandler) *WorkerService {
	return &WorkerService{
		handler: handler,
	}
}

// Register does nothing in this example, but let's keep it to satisfy the Service interface
func (s *WorkerService) Register(mux *http.ServeMux) {
	// Could register endpoints for worker metrics or status, e.g.,
	// mux.HandleFunc("/worker/status", s.statusHandler)
	// For now, no endpoints exposed.
}
