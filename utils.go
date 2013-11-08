package main

import (
	"math/rand"
)

const (
	MaxInt = int(^uint(0) >> 1)
)

func Abs(v int) int {
	if v < 0 {
		return -v
	} else {
		return v
	}
}

func Shuffle(a []int) {
	for i := range a {
		j := i + rand.Intn(len(a)-i)
		a[i], a[j] = a[j], a[i]
	}
}

func SpreadSum(sum int, n int) (result []int) {
	result = make([]int, n)

	quot := sum / n
	rem := sum % n

	for i := range result {
		result[i] = quot
		if rem != 0 {
			rem -= 1
			result[i] += 1
		}
	}

	Shuffle(result)

	return
}

func B2i(b bool) int {
	if b {
		return 1
	} else {
		return 0
	}
}

func Min(a, b int) int {
	if a <= b {
		return a
	} else {
		return b
	}
}

func Reverse(a []int) {
	n := len(a)
	for i := 0; i < n/2; i++ {
		a[i], a[n-i-1] = a[n-i-1], a[i]
	}
}

func KSubsets(n, k int, f func([]int)) {
	if k > n {
		panic("k cannot be larger than n")
	}

	subset := make([]int, k)
	for i := 0; i < k; i++ {
		subset[i] = i
	}

	for {
		f(subset)

		var i int
		prev := n
		for i = k - 1; i >= 0; i-- {
			if subset[i]+1 < prev {
				subset[i]++
				for j := i + 1; j < k; j++ {
					subset[j] = subset[j-1] + 1
				}

				break
			}

			prev = subset[i]
		}

		if i < 0 {
			break
		}
	}
}

func Permutations(n int, f func([]int)) {
	perm := make([]int, n)
	for i := 0; i < n; i++ {
		perm[i] = i
	}

	for {
		f(perm)

		var i int
		for i = n - 2; i >= 0; i-- {
			if perm[i] < perm[i+1] {
				break
			}
		}

		if i < 0 {
			break
		}

		var j int
		for j = n - 1; j > i; j-- {
			if perm[j] > perm[i] {
				break
			}
		}

		if j <= i {
			panic("cannot happen")
		}

		perm[i], perm[j] = perm[j], perm[i]
		Reverse(perm[i+1:])
	}
}
