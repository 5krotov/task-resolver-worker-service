package domain

// TaskRepository To interact with Kafka
type TaskRepository interface {
	ConsumeTasks() (<-chan *Task, <-chan error) // returns a read-only channel of tasks and a read-only channel of errors
	ProduceResult(task *Task) error             // publishes the processed task (result) back to Kafka
}
