package worker


type WorkerPool[T any, V any] struct {
	Size 		int
	Done 		chan V
	WorkerMap	map[int]chan T
}

func NewWorkerPool[T any, V any](size int, done chan V) *WorkerPool[T,V] {
	wm := map[int]chan T{}

	for i := 0; i < size; i++ {
		wm[i] = make(chan T)
	}

	return &WorkerPool[T,V]{
		Size: size,
		Done: done,
		WorkerMap: wm,
	}

}

func (wp *WorkerPool[T,V])InitWorkers(task func(j T) V) {

	for _,v := range wp.WorkerMap {
		go worker[T,V](
			func(j T) V{
				return task(j)
			}, v, wp.Done)

	}

}

func (wp *WorkerPool[T, V])Close() {
	for _, v := range wp.WorkerMap {
		close(v)
	}
}


func worker[T any, V any](task func(s T) V, jobs <-chan T, done chan<- V) {

	for j := range jobs {
		res := task(j)
		done <- res
	}

}


