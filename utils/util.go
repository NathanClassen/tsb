package utils

func CalculateMedian(of []int) int {
	l := len(of)
	mid := l / 2
	if l&1 == 1 {
		return of[mid]
	} else {
		return (of[mid -1] + of[mid]) / 2
	}
}

func CalculateAvg(of []int) int {
	return Sum(of) / len(of)
}

func Sum(all []int) int {
	sum := 0
	for _, num := range all {
		sum += num

	}
	return sum
}
