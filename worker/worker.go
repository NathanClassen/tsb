package worker

import (
	"sync"
)

type WorkerPool[T any, V any] struct {
	Size      int
	Done      chan V
	Closed    bool
	workerMap map[int]chan T
}

func NewWorkerPool[T any, V any](size int, done chan V) *WorkerPool[T, V] {
	wm := map[int]chan T{}

	for i := 0; i < size; i++ {
		wm[i] = make(chan T)
	}

	return &WorkerPool[T, V]{
		Size:      size,
		Done:      done,
		workerMap: wm,
	}

}

func (wp *WorkerPool[T, V]) SendJob(mu *sync.Mutex, workerId int, job T) {
	wc := wp.workerMap[workerId]

	//	prevents sending to close channels if error is encountered, also throttles the
	//	workers a bit resulting in improved query performance but longer program exectution
	mu.Lock()
	wc <- job
	mu.Unlock()

}

func (wp *WorkerPool[T, V]) InitWorkers(task func(j T) V) {

	for _, v := range wp.workerMap {
		go wp.worker(task, v, wp.Done)
	}

}

func (wp *WorkerPool[T, V]) Close() {
	if !wp.Closed {
		for _, v := range wp.workerMap {
			close(v)
		}
	}
	wp.Closed = true
}

func (wp *WorkerPool[T, V]) worker(task func(s T) V, jobs <-chan T, done chan<- V) {
	for j := range jobs {

		if !wp.Closed {
			res := task(j)
			done <- res
		}

	}
}
