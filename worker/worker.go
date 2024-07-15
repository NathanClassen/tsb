package worker

import (
	"context"
	"log"
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

func (wp *WorkerPool[T, V]) SendJob(workerId int, job T) {
	if wp.Closed {
		log.Println("warning: tried sending job to closed pool")
		return
	}
	wc := wp.workerMap[workerId]
	wc <- job
}

func (wp *WorkerPool[T, V]) InitWorkers(ctx context.Context, task func(j T) V) {

	for _, v := range wp.workerMap {
		go wp.worker(ctx, task, v, wp.Done)
	}

}

func (wp *WorkerPool[T, V]) Close() {
	wp.Closed = true
	for _, v := range wp.workerMap {
		close(v)
	}
}

func (wp *WorkerPool[T, V]) worker(ctx context.Context, task func(s T) V, jobs <-chan T, done chan<- V) {

	for {
		select {
		case j := <-jobs: //	TODO: can this be removed after cleaning error handling?
			//	having strange issue where if a non-existent file is given as input, this channel
			//	sometimes recieves zero-time.Time value..
			if !wp.Closed {
				res := task(j)
				done <- res
			}
		case <-ctx.Done():
			return
		}
	}
}
