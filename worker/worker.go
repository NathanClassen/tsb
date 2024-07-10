package worker

import (
	"fmt"
)


type WorkerPool[T any, V any] struct {
	Size 		int
	Done 		chan V
	workerMap	map[int]chan T
}

func NewWorkerPool[T any, V any](size int, done chan V) *WorkerPool[T,V] {
	wm := map[int]chan T{}

	for i := 0; i < size; i++ {
		wm[i] = make(chan T)
	}

	return &WorkerPool[T,V]{
		Size: size,
		Done: done,
		workerMap: wm,
	}

}

func (wp *WorkerPool[T, V])SendJob(workerId int, job T) {
	wc, found := wp.workerMap[workerId]
	if !found {
		fmt.Printf("could not find work channel in %v for %d\n\n",wp.workerMap,workerId)
	}
	
	wc <- job
}

func (wp *WorkerPool[T,V])InitWorkers(task func(j T) V) {

	for _,v := range wp.workerMap {
		go worker[T,V](
			func(j T) V{
				return task(j)
			}, v, wp.Done)

	}

}

func (wp *WorkerPool[T, V])Close() {
	for _, v := range wp.workerMap {
		close(v)
	}
}


func worker[T any, V any](task func(s T) V, jobs <-chan T, done chan<- V) {

	for j := range jobs {
		res := task(j)
		done <- res
	}

}


