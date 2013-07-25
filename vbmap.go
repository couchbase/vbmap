package main

import (
	"bytes"
	"container/heap"
	"fmt"
	"math/rand"
)

type Node int
type Tag uint
type TagMap map[Node]Tag

type VbmapParams struct {
	Tags TagMap

	NumNodes    int
	NumSlaves   int
	NumVBuckets int
	NumReplicas int
}

func (tags TagMap) String() string {
	return fmt.Sprintf("%v", map[Node]Tag(tags))
}

func (tags TagMap) TagsCount() int {
	seen := make(map[Tag]bool)

	for _, t := range tags {
		seen[t] = true
	}

	return len(seen)
}

type RI [][]int

type RIGenerator interface {
	Generate(params VbmapParams) (RI, error)
	fmt.Stringer
}

func (RI RI) String() string {
	buffer := &bytes.Buffer{}

	for _, row := range RI {
		for _, elem := range row {
			fmt.Fprintf(buffer, "%2d ", elem)
		}
		fmt.Fprintf(buffer, "\n")
	}

	return buffer.String()
}

type RCandidate struct {
	params VbmapParams
	matrix [][]int

	rowSums          []int
	colSums          []int
	expectedColSum   int
	expectedOutliers int

	outliers      int
	rawEvaluation int
}

func (cand RCandidate) String() string {
	buffer := &bytes.Buffer{}

	fmt.Fprintf(buffer, "    |")
	for i := 0; i < cand.params.NumNodes; i++ {
		fmt.Fprintf(buffer, "%3d ", cand.params.Tags[Node(i)])
	}
	fmt.Fprintf(buffer, "|\n")

	fmt.Fprintf(buffer, "----|")
	for i := 0; i < cand.params.NumNodes; i++ {
		fmt.Fprintf(buffer, "----")
	}
	fmt.Fprintf(buffer, "|\n")

	for i, row := range cand.matrix {
		fmt.Fprintf(buffer, "%3d |", cand.params.Tags[Node(i)])
		for _, elem := range row {
			fmt.Fprintf(buffer, "%3d ", elem)
		}
		fmt.Fprintf(buffer, "| %d\n", cand.rowSums[i])
	}

	fmt.Fprintf(buffer, "____|")
	for i := 0; i < cand.params.NumNodes; i++ {
		fmt.Fprintf(buffer, "____")
	}
	fmt.Fprintf(buffer, "|\n")

	fmt.Fprintf(buffer, "    |")
	for i := 0; i < cand.params.NumNodes; i++ {
		fmt.Fprintf(buffer, "%3d ", cand.colSums[i])
	}
	fmt.Fprintf(buffer, "|\n")
	fmt.Fprintf(buffer, "Evaluation: %d\n", cand.evaluation())

	return buffer.String()
}

func buildInitialR(params VbmapParams, RI [][]int) (R [][]int) {
	activeVbsPerNode := SpreadSum(params.NumVBuckets, params.NumNodes)

	R = make([][]int, len(RI))
	if params.NumSlaves == 0 {
		return
	}

	for i, row := range RI {
		rowSum := activeVbsPerNode[i] * params.NumReplicas
		slaveVbs := SpreadSum(rowSum, params.NumSlaves)

		R[i] = make([]int, len(row))

		slave := 0
		for j, elem := range row {
			if elem != 0 {
				R[i][j] = slaveVbs[slave]
				slave += 1
			}
		}
	}

	return
}

func makeRCandidate(params VbmapParams, RI [][]int) (result RCandidate) {
	result.params = params
	result.matrix = buildInitialR(params, RI)
	result.colSums = make([]int, params.NumNodes)
	result.rowSums = make([]int, params.NumNodes)

	for i, row := range result.matrix {
		rowSum := 0
		for j, elem := range row {
			rowSum += elem
			result.colSums[j] += elem
		}
		result.rowSums[i] = rowSum
	}

	numReplications := params.NumVBuckets * params.NumReplicas
	result.expectedColSum = numReplications / params.NumNodes
	result.expectedOutliers = numReplications % params.NumNodes

	for _, sum := range result.colSums {
		result.rawEvaluation += Abs(sum - result.expectedColSum)
		if sum == result.expectedColSum+1 {
			result.outliers += 1
		}
	}

	return
}

func (cand RCandidate) computeEvaluation(rawEval int, outliers int) (eval int) {
	eval = rawEval
	if outliers > cand.expectedOutliers {
		eval -= cand.expectedOutliers
	} else {
		eval -= outliers
	}

	return
}

func (cand RCandidate) evaluation() int {
	return cand.computeEvaluation(cand.rawEvaluation, cand.outliers)
}

func (cand RCandidate) swapOutliersChange(row int, j int, k int) (change int) {
	a, b := cand.matrix[row][j], cand.matrix[row][k]
	ca := cand.colSums[j] - a + b
	cb := cand.colSums[k] - b + a

	if cand.colSums[j] == cand.expectedColSum+1 {
		change -= 1
	}
	if cand.colSums[k] == cand.expectedColSum+1 {
		change -= 1
	}
	if ca == cand.expectedColSum+1 {
		change += 1
	}
	if cb == cand.expectedColSum+1 {
		change += 1
	}

	return
}

func (cand RCandidate) swapRawEvaluationChange(row int, j int, k int) (change int) {
	a, b := cand.matrix[row][j], cand.matrix[row][k]
	ca := cand.colSums[j] - a + b
	cb := cand.colSums[k] - b + a

	evalA := Abs(ca - cand.expectedColSum)
	evalB := Abs(cb - cand.expectedColSum)

	oldEvalA := Abs(cand.colSums[j] - cand.expectedColSum)
	oldEvalB := Abs(cand.colSums[k] - cand.expectedColSum)

	change = evalA - oldEvalA + evalB - oldEvalB

	return
}

func (cand RCandidate) swapBenefit(row int, j int, k int) int {
	eval := cand.evaluation()

	swapOutliers := cand.outliers + cand.swapOutliersChange(row, j, k)
	swapRawEval := cand.rawEvaluation + cand.swapRawEvaluationChange(row, j, k)
	swapEval := cand.computeEvaluation(swapRawEval, swapOutliers)

	return swapEval - eval
}

func (cand *RCandidate) swapElems(row int, j int, k int) {
	if cand.matrix[row][j] == 0 || cand.matrix[row][k] == 0 {
		panic(fmt.Sprintf("swapping one or more zeros (%d: %d <-> %d)",
			row, j, k))
	}

	cand.rawEvaluation += cand.swapRawEvaluationChange(row, j, k)
	cand.outliers += cand.swapOutliersChange(row, j, k)

	a, b := cand.matrix[row][j], cand.matrix[row][k]

	cand.colSums[j] += b - a
	cand.colSums[k] += a - b
	cand.matrix[row][j], cand.matrix[row][k] = b, a
}

func (cand RCandidate) copy() (result RCandidate) {
	result.params = cand.params
	result.expectedColSum = cand.expectedColSum
	result.expectedOutliers = cand.expectedOutliers
	result.outliers = cand.outliers
	result.rawEvaluation = cand.rawEvaluation

	result.matrix = make([][]int, cand.params.NumNodes)
	for i, row := range cand.matrix {
		result.matrix[i] = make([]int, cand.params.NumNodes)
		copy(result.matrix[i], row)
	}

	result.rowSums = make([]int, cand.params.NumNodes)
	copy(result.rowSums, cand.rowSums)

	result.colSums = make([]int, cand.params.NumNodes)
	copy(result.colSums, cand.colSums)

	return
}

type TabuPair struct {
	row, j, k int
}

type TabuElem struct {
	row, col int
}

type Tabu struct {
	tabu        map[TabuPair]int
	elemIndex   map[TabuElem]TabuPair
	expireIndex map[int]TabuPair
}

func makeTabuPair(row int, j int, k int) TabuPair {
	if j > k {
		j, k = k, j
	}
	return TabuPair{row, j, k}
}

func makeTabu() Tabu {
	return Tabu{make(map[TabuPair]int),
		make(map[TabuElem]TabuPair),
		make(map[int]TabuPair)}
}

func (tabu Tabu) add(time int, row int, j int, k int) {
	oldItem, present := tabu.elemIndex[TabuElem{row, j}]
	if present {
		tabu.expire(tabu.tabu[oldItem])
	}

	oldItem, present = tabu.elemIndex[TabuElem{row, k}]
	if present {
		tabu.expire(tabu.tabu[oldItem])
	}

	item := makeTabuPair(row, j, k)
	tabu.tabu[item] = time
	tabu.expireIndex[time] = item

	tabu.elemIndex[TabuElem{row, j}] = item
	tabu.elemIndex[TabuElem{row, k}] = item
}

func (tabu Tabu) member(row int, j int, k int) bool {
	_, present := tabu.tabu[makeTabuPair(row, j, k)]
	return present
}

func (tabu Tabu) expire(time int) {
	item := tabu.expireIndex[time]
	delete(tabu.expireIndex, time)
	delete(tabu.tabu, item)
	delete(tabu.elemIndex, TabuElem{item.row, item.j})
	delete(tabu.elemIndex, TabuElem{item.row, item.k})
}

func doBuildR(params VbmapParams, RI [][]int) (best RCandidate) {
	cand := makeRCandidate(params, RI)
	best = cand.copy()

	if params.NumSlaves <= 1 || params.NumReplicas == 0 {
		// nothing to optimize here; just return
		return
	}

	attempts := 10 * params.NumNodes * params.NumNodes
	expire := 10 * params.NumNodes
	noImprovementLimit := params.NumNodes * params.NumNodes

	highElems := make([]int, params.NumNodes)
	lowElems := make([]int, params.NumNodes)

	candidateRows := make([]int, params.NumNodes)
	tabu := makeTabu()

	noCandidate := 0
	swapTabued := 0
	swapDecreased := 0
	swapIndifferent := 0
	swapIncreased := 0

	t := 0
	noImprovementIters := 0

	for t = 0; t < attempts; t++ {
		if t >= expire {
			tabu.expire(t - expire)
		}

		if noImprovementIters >= noImprovementLimit {
			break
		}

		noImprovementIters++

		if best.evaluation() == 0 {
			break
		}

		highElems = []int{}
		lowElems = []int{}

		candidateRows = []int{}

		for i, elem := range cand.colSums {
			switch {
			case elem <= cand.expectedColSum:
				lowElems = append(lowElems, i)
			case elem > cand.expectedColSum:
				highElems = append(highElems, i)
			}
		}

		lowIx := lowElems[rand.Intn(len(lowElems))]
		highIx := highElems[rand.Intn(len(highElems))]

		for row := 0; row < params.NumNodes; row++ {
			lowElem := cand.matrix[row][lowIx]
			highElem := cand.matrix[row][highIx]

			if lowElem != 0 && highElem != 0 && highElem != lowElem {
				benefit := cand.swapBenefit(row, lowIx, highIx)

				if benefit >= 0 && rand.Intn(20) != 0 {
					continue
				}

				candidateRows = append(candidateRows, row)
			}
		}

		if len(candidateRows) == 0 {
			noCandidate++
			continue
		}

		row := candidateRows[rand.Intn(len(candidateRows))]

		if tabu.member(row, lowIx, highIx) {
			swapTabued++
			continue
		}

		old := cand.evaluation()

		cand.swapElems(row, lowIx, highIx)
		tabu.add(t, row, lowIx, highIx)

		if old == cand.evaluation() {
			swapIndifferent++
		} else if old < cand.evaluation() {
			swapIncreased++
		} else {
			swapDecreased++
		}

		if cand.evaluation() < best.evaluation() {
			best = cand.copy()
			noImprovementIters = 0
		}
	}

	diag.Printf("Search stats")
	diag.Printf("  iters -> %d", t)
	diag.Printf("  no improvement termination? -> %v",
		noImprovementIters >= noImprovementLimit)
	diag.Printf("  noCandidate -> %d", noCandidate)
	diag.Printf("  swapTabued -> %d", swapTabued)
	diag.Printf("  swapDecreased -> %d", swapDecreased)
	diag.Printf("  swapIndifferent -> %d", swapIndifferent)
	diag.Printf("  swapIncreased -> %d", swapIncreased)
	diag.Printf("")

	return
}

func buildR(params VbmapParams, RI [][]int) (best RCandidate) {
	bestEvaluation := (1 << 31) - 1

	for i := 0; i < 10; i++ {
		R := doBuildR(params, RI)
		if R.evaluation() < bestEvaluation {
			best = R
			bestEvaluation = R.evaluation()
		}

		if bestEvaluation == 0 {
			diag.Printf("Found balanced map R after %d attempts", i)
			break
		}
	}

	if bestEvaluation != 0 {
		diag.Printf("Failed to find balanced map R (best evaluation %d)",
			bestEvaluation)
	}

	return
}

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

type Slave struct {
	index   int
	count   int
	numUsed int
}

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

type IndexPair struct {
	x, y int
}

func getCount(counts map[IndexPair]int, x int, y int) (count int) {
	count, present := counts[IndexPair{x, y}]
	if !present {
		count = 0
	}

	return
}

func incCount(counts map[IndexPair]int, x int, y int) {
	count := getCount(counts, x, y)
	counts[IndexPair{x, y}] = count + 1
}

// Choose numReplicas replicas out of candidates array based on counts.
//
// It does so by prefering a replica r with the lowest count for pair {prev,
// r} in counts. prev is either -1 (which means master node) or replica from
// previous turn.
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
		var pair *IndexPair = nil
		var cost int

		processPair := func(x, y int) {
			if x == y {
				return
			}

			cand := IndexPair{x, y}
			candCost := getCount(counts, x, y)

			if pair == nil {
				pair = &cand
				cost = candCost
			}

			if candCost < cost {
				pair = &cand
				cost = candCost
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

		if pair == nil {
			panic("couldn't find a pair")
		}

		resultIxs[i] = pair.y
		delete(available, pair.y)
	}

	for i, r := range resultIxs {
		result[i] = candidatesMap[r]
	}

	for s, _ := range available {
		intact = append(intact, candidatesMap[s])
	}

	return
}

func buildVbmap(R RCandidate) (vbmap Vbmap) {
	params := R.params
	vbmap = makeVbmap(params)

	var nodeVbs []int
	if params.NumReplicas == 0 || params.NumSlaves == 0 {
		nodeVbs = SpreadSum(params.NumVBuckets, params.NumNodes)
	} else {
		nodeVbs = make([]int, params.NumNodes)
		for i, sum := range R.rowSums {
			vbs := sum / params.NumReplicas
			if sum%params.NumReplicas != 0 {
				panic("row sum is not multiple of NumReplicas")
			}

			nodeVbs[i] = vbs
		}
	}

	vbucket := 0
	for i, row := range R.matrix {
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
					different = different || (slave.count == lastCount)
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

			for _, slave := range intact {
				heap.Push(slaves, slave)
			}

			vbs--
			vbucket++
		}
	}

	return
}

func VbmapGenerate(params VbmapParams, gen RIGenerator) (vbmap Vbmap, err error) {
	RI, err := gen.Generate(params)
	if err != nil {
		return
	}

	diag.Printf("Generated topology:\n%s", RI.String())

	R := buildR(params, RI)

	diag.Printf("Final map R:\n%s", R.String())

	return buildVbmap(R), nil
}
