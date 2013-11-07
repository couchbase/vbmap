package main

import (
	"bytes"
	"container/heap"
	"fmt"
	"math/rand"
	"sort"
)

type Vbmap [][]Node

func (vbmap Vbmap) String() string {
	buffer := &bytes.Buffer{}

	for i, nodes := range vbmap {
		fmt.Fprintf(buffer, "%4d: ", i)
		for _, n := range nodes {
			fmt.Fprintf(buffer, "%3d ", n)
		}
		fmt.Fprintf(buffer, "\n")
	}

	return buffer.String()
}

func makeVbmap(params VbmapParams) (vbmap Vbmap) {
	vbmap = make([][]Node, params.NumVBuckets)
	for v := 0; v < params.NumVBuckets; v++ {
		vbmap[v] = make([]Node, params.NumReplicas+1)
	}

	return
}

// Represents a slave node.
type Slave struct {
	index   int // column index of this slave in corresponding row of R
	count   int // number of vbuckets that can be put on this slave
	numUsed int // number of times this slave was used so far
}

// A heap of slaves of some node. Slaves in the heap are ordered in the
// descending order of number of vbuckets slots left on these slaves. If for
// some pair of nodes number of vbuckets left is identical, then the node that
// has been used less is preferred.
type SlaveHeap []Slave

func makeSlave(index int, count int, params VbmapParams) (slave Slave) {
	slave.index = index
	slave.count = count
	return
}

func (h SlaveHeap) Len() int {
	return len(h)
}

func (h SlaveHeap) Less(i, j int) (result bool) {
	switch {
	case h[i].count > h[j].count:
		result = true
	case h[i].count == h[j].count:
		result = h[i].numUsed < h[j].numUsed
	default:
		result = false
	}

	return
}

func (h SlaveHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *SlaveHeap) Push(x interface{}) {
	*h = append(*h, x.(Slave))
}

func (h *SlaveHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// A pair of column indexes of two elements in the same row of matrix R. Used
// to track how many times certain pair of nodes is used adjacently to
// replicate some active vbucket from a certain node.
type IndexPair struct {
	x, y int
}

// Get current usage count for slaves x and y.
func getCount(counts map[IndexPair]int, x int, y int) (count int) {
	count, present := counts[IndexPair{x, y}]
	if !present {
		count = 0
	}

	return
}

// Increment usage count for slave x and y.
func incCount(counts map[IndexPair]int, x int, y int) {
	count := getCount(counts, x, y)
	counts[IndexPair{x, y}] = count + 1
}

// Choose numReplicas replicas out of candidates array based on counts.
//
// It does so by prefering a replica r with the lowest count for pair {prev,
// r} in counts. prev is either -1 (which means master node) or replica from
// previous turn. If for several possible replicas counts are the same then
// one of them is selected uniformly.
func chooseReplicas(candidates []Slave,
	numReplicas int, counts map[IndexPair]int) (result []Slave, intact []Slave) {

	result = make([]Slave, numReplicas)
	resultIxs := make([]int, numReplicas)
	intact = make([]Slave, len(candidates)-numReplicas)[:0]

	candidatesMap := make(map[int]Slave)
	available := make(map[int]bool)

	for _, r := range candidates {
		candidatesMap[r.index] = r
		available[r.index] = true
	}

	for i := 0; i < numReplicas; i++ {
		var possibleReplicas []int = nil
		var cost int

		processPair := func(x, y int) {
			if x == y {
				return
			}

			cand := y
			candCost := getCount(counts, x, y)

			if possibleReplicas == nil {
				cost = candCost
			}

			if candCost < cost {
				possibleReplicas = append([]int(nil), cand)
				cost = candCost
			} else if candCost == cost {
				possibleReplicas = append(possibleReplicas, cand)
			}
		}

		var prev int
		if i == 0 {
			// master
			prev = -1
		} else {
			prev = resultIxs[i-1]
		}

		for x, _ := range available {
			processPair(prev, x)
		}

		if possibleReplicas == nil {
			panic("couldn't find a replica")
		}

		sort.Ints(possibleReplicas)
		replica := possibleReplicas[rand.Intn(len(possibleReplicas))]

		resultIxs[i] = replica
		delete(available, replica)
	}

	for i, r := range resultIxs {
		result[i] = candidatesMap[r]
	}

	for s, _ := range available {
		intact = append(intact, candidatesMap[s])
	}

	return
}

// Construct vbucket map from a matrix R.
func buildVbmap(R R) (vbmap Vbmap) {
	params := R.params
	vbmap = makeVbmap(params)

	// determines how many active vbuckets each node has
	var nodeVbs []int
	if params.NumReplicas == 0 || params.NumSlaves == 0 {
		// If there's only one copy of every vbucket, then matrix R is
		// just a null matrix. So we just spread the vbuckets evenly
		// among the nodes and we're almost done.
		nodeVbs = SpreadSum(params.NumVBuckets, params.NumNodes)
	} else {
		// Otherwise matrix R defines the amount of active vbuckets
		// each node has.
		nodeVbs = make([]int, params.NumNodes)
		for i, sum := range R.RowSums {
			vbs := sum / params.NumReplicas
			if sum%params.NumReplicas != 0 {
				panic("row sum is not multiple of NumReplicas")
			}

			nodeVbs[i] = vbs
		}
	}

	vbucket := 0
	for i, row := range R.Matrix {
		slaves := &SlaveHeap{}
		counts := make(map[IndexPair]int)

		heap.Init(slaves)

		vbs := nodeVbs[i]

		for s, count := range row {
			if count != 0 {
				heap.Push(slaves, makeSlave(s, count, params))
			}
		}

		if slaves.Len() == 0 {
			// Row matrix contained only zeros. This usually means
			// that replica count is zero. Other possibility is
			// that the number of vbuckets is less than number of
			// nodes and some of the nodes end up with no vbuckets
			// at all. In any case, we just mark the node as a
			// master for its vbuckets (if any).

			for vbs > 0 {
				vbmap[vbucket][0] = Node(i)
				vbs--
				vbucket++
			}

			continue
		}

		// We're selecting possible candidates for this particular
		// replica chain. To ensure that we don't end up in a
		// situation when there's only one slave left in the heap and
		// it's count is greater than one, we always pop slaves with
		// maximum count of vbuckets left first (see SlaveHeap.Less()
		// method for details). When counts are the same, node that
		// has been used less is preferred. We try to select more
		// candidates than the number of replicas we need. This is to
		// have more freedom when selecting actual replicas. For
		// details on this look at chooseReplicas() function.
		candidates := make([]Slave, params.NumSlaves)
		for vbs > 0 {
			candidates = nil
			vbmap[vbucket][0] = Node(i)

			var lastCount int
			var different bool = false

			for r := 0; r < params.NumReplicas; r++ {
				if slaves.Len() == 0 {
					panic("Ran out of slaves")
				}

				slave := heap.Pop(slaves).(Slave)
				candidates = append(candidates, slave)

				if r > 0 {
					different = different || (slave.count != lastCount)
				}

				lastCount = slave.count
			}

			// If candidates that we selected so far have
			// different counts, to simplify chooseReplicas()
			// logic we don't try select other candidates. This is
			// needed because all the candidates with higher
			// counts has to be selected by
			// chooseReplicas(). Otherwise it would be possible to
			// end up with a single slave with count greater than
			// one in the heap.
			if !different {
				for {
					if slaves.Len() == 0 {
						break
					}

					// We add more slaves to the candidate
					// list while all of them has the same
					// count of vbuckets left.
					slave := heap.Pop(slaves).(Slave)
					if slave.count == lastCount {
						candidates = append(candidates, slave)
					} else {
						heap.Push(slaves, slave)
						break
					}
				}
			}

			// Here we just choose actual replicas for this
			// vbucket out of candidates. We adjust usage stats
			// for chosen slaves. And push them back to the heap
			// if their vbuckets left count is non-zero.
			replicas, intact := chooseReplicas(candidates, params.NumReplicas, counts)

			for turn, slave := range replicas {
				slave.count--
				slave.numUsed++

				vbmap[vbucket][turn+1] = Node(slave.index)

				if slave.count != 0 {
					heap.Push(slaves, slave)
				}

				var prev int
				if turn == 0 {
					// this means master
					prev = -1
				} else {
					prev = replicas[turn-1].index
				}

				incCount(counts, prev, slave.index)
			}

			// just push all the unused slaves back to the heap
			for _, slave := range intact {
				heap.Push(slaves, slave)
			}

			vbs--
			vbucket++
		}
	}

	return
}

// Generate vbucket map given a generator for matrix RI and vbucket map
// parameters.
func VbmapGenerate(params VbmapParams, gen RIGenerator) (vbmap Vbmap, err error) {
	RI, err := gen.Generate(params)
	if err != nil {
		return
	}

	diag.Printf("Generated topology:\n%s", RI.String())

	R := BuildR(params, RI)
	diag.Printf("Final map R:\n%s", R.String())

	return buildVbmap(R), nil
}
