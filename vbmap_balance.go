package main

import (
	"bytes"
	"fmt"
)

// Matrix R with some meta-information.
type R struct {
	Matrix  [][]int // actual matrix
	RowSums []int   // row sums for the matrix
	ColSums []int   // column sums for the matrix
	Strict  bool

	params           VbmapParams // corresponding vbucket map params
	expectedColSum   int         // expected column sum
	expectedOutliers int         // number of columns that can be off-by-one

	outliers int // actual number of columns that are off-by-one

	// sum of absolute differences between column sum and expected column
	// sum; it's called 'raw' because it doesn't take into consideration
	// that expectedOutliers number of columns can be off-by-one; the
	// smaller the better
	rawEvaluation int
}

func (cand R) String() string {
	buffer := &bytes.Buffer{}

	nodes := cand.params.Nodes()

	fmt.Fprintf(buffer, "    |")
	for _, node := range nodes {
		fmt.Fprintf(buffer, "%3d ", cand.params.Tags[node])
	}
	fmt.Fprintf(buffer, "|\n")

	fmt.Fprintf(buffer, "----|")
	for _ = range nodes {
		fmt.Fprintf(buffer, "----")
	}
	fmt.Fprintf(buffer, "|\n")

	for i, row := range cand.Matrix {
		fmt.Fprintf(buffer, "%3d |", cand.params.Tags[Node(i)])
		for _, elem := range row {
			fmt.Fprintf(buffer, "%3d ", elem)
		}
		fmt.Fprintf(buffer, "| %d\n", cand.RowSums[i])
	}

	fmt.Fprintf(buffer, "____|")
	for _ = range nodes {
		fmt.Fprintf(buffer, "____")
	}
	fmt.Fprintf(buffer, "|\n")

	fmt.Fprintf(buffer, "    |")
	for i := range nodes {
		fmt.Fprintf(buffer, "%3d ", cand.ColSums[i])
	}
	fmt.Fprintf(buffer, "|\n")
	fmt.Fprintf(buffer, "Evaluation: %d\n", cand.evaluation())

	return buffer.String()
}

// Build initial matrix R from RI.
//
// It just spreads active vbuckets uniformly between the nodes. And then for
// each node spreads replica vbuckets among the slaves of this node.
func buildInitialR(params VbmapParams, ri RI) (r [][]int) {
	activeVbsPerNode := SpreadSum(params.NumVBuckets, params.NumNodes)

	r = make([][]int, len(ri.Matrix))
	if params.NumSlaves == 0 {
		return
	}

	for i, row := range ri.Matrix {
		rowSum := activeVbsPerNode[i] * params.NumReplicas
		slaveVbs := SpreadSum(rowSum, params.NumSlaves)

		r[i] = make([]int, len(row))

		slave := 0
		for j, elem := range row {
			if elem {
				r[i][j] = slaveVbs[slave]
				slave += 1
			}
		}
	}

	return
}

func buildRFlowGraph(params VbmapParams, ri RI, activeVbs []int) (g *Graph) {
	graphName := fmt.Sprintf("Flow graph for R (%s)", params)
	g = NewGraph(graphName)

	colSum := params.NumVBuckets * params.NumReplicas / params.NumNodes

	for i, row := range ri.Matrix {
		node := Node(i)
		nodeSrcV := NodeSourceVertex(node)
		nodeSinkV := NodeSinkVertex(node)

		rowSum := activeVbs[i] * params.NumReplicas

		g.AddEdge(Source, nodeSrcV, rowSum, rowSum)
		g.AddEdge(nodeSinkV, Sink, colSum+1, colSum)

		seenTags := make(map[Tag]bool)
		for j, elem := range row {
			if !elem {
				continue
			}

			dstNode := Node(j)
			dstNodeTag := params.Tags[dstNode]

			dstNodeSinkV := NodeSinkVertex(dstNode)
			tagV := TagNodeVertex{dstNodeTag, node}

			if _, seen := seenTags[dstNodeTag]; !seen {
				maxVbsPerTag := rowSum / params.NumReplicas

				g.AddEdge(nodeSrcV, tagV, maxVbsPerTag, 0)
				seenTags[dstNodeTag] = true
			}

			elem := rowSum / params.NumSlaves
			g.AddEdge(tagV, dstNodeSinkV, elem+1, elem)
		}
	}

	return
}

func relaxMaxVbsPerTag(g *Graph) {
	for _, vertex := range g.Vertices() {
		if tagV, ok := vertex.(TagNodeVertex); ok {
			for _, edge := range g.EdgesToVertex(tagV) {
				edge.IncreaseCapacity(MaxInt)
			}
		}
	}
}

func graphToR(g *Graph, params VbmapParams) (r R) {
	matrix := make([][]int, params.NumNodes)

	for i := range matrix {
		matrix[i] = make([]int, params.NumNodes)
	}

	for _, dst := range params.Nodes() {
		dstVertex := NodeSinkVertex(dst)

		for _, edge := range g.EdgesToVertex(dstVertex) {
			src := edge.Src.(TagNodeVertex).Node

			matrix[int(src)][int(dst)] = edge.Flow()
		}
	}

	return makeRFromMatrix(params, matrix)
}

// Construct initial matrix R from RI.
//
// Uses buildInitialR to construct R.
func makeR(params VbmapParams, ri RI) (result R) {
	matrix := buildInitialR(params, ri)
	return makeRFromMatrix(params, matrix)
}

func makeRFromMatrix(params VbmapParams, matrix [][]int) (result R) {
	result.params = params
	result.Matrix = matrix
	result.ColSums = make([]int, params.NumNodes)
	result.RowSums = make([]int, params.NumNodes)

	for i, row := range result.Matrix {
		rowSum := 0
		for j, elem := range row {
			rowSum += elem
			result.ColSums[j] += elem
		}
		result.RowSums[i] = rowSum
	}

	numReplications := params.NumVBuckets * params.NumReplicas
	result.expectedColSum = numReplications / params.NumNodes
	result.expectedOutliers = numReplications % params.NumNodes

	for _, sum := range result.ColSums {
		result.rawEvaluation += Abs(sum - result.expectedColSum)
		if sum == result.expectedColSum+1 {
			result.outliers += 1
		}
	}

	return
}

// Compute adjusted evaluation of matrix R from raw evaluation and number of
// outliers.
//
// It differs from raw evaluation in that it allows fixed number of column
// sums to be off-by-one.
func (cand R) computeEvaluation(rawEval int, outliers int) (eval int) {
	eval = rawEval
	if outliers > cand.expectedOutliers {
		eval -= cand.expectedOutliers
	} else {
		eval -= outliers
	}

	return
}

// Compute adjusted evaluation of matrix R.
func (cand R) evaluation() int {
	return cand.computeEvaluation(cand.rawEvaluation, cand.outliers)
}

// Compute a change in number of outlying elements after swapping elements j
// and k in certain row.
func (cand R) swapOutliersChange(row int, j int, k int) (change int) {
	a, b := cand.Matrix[row][j], cand.Matrix[row][k]
	ca := cand.ColSums[j] - a + b
	cb := cand.ColSums[k] - b + a

	if cand.ColSums[j] == cand.expectedColSum+1 {
		change -= 1
	}
	if cand.ColSums[k] == cand.expectedColSum+1 {
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

// Compute a change in rawEvaluation after swapping elements j and k in
// certain row.
func (cand R) swapRawEvaluationChange(row int, j int, k int) (change int) {
	a, b := cand.Matrix[row][j], cand.Matrix[row][k]
	ca := cand.ColSums[j] - a + b
	cb := cand.ColSums[k] - b + a

	evalA := Abs(ca - cand.expectedColSum)
	evalB := Abs(cb - cand.expectedColSum)

	oldEvalA := Abs(cand.ColSums[j] - cand.expectedColSum)
	oldEvalB := Abs(cand.ColSums[k] - cand.expectedColSum)

	change = evalA - oldEvalA + evalB - oldEvalB

	return
}

// Compute a potential change in evaluation after swapping element j and k in
// certain row.
func (cand R) swapBenefit(row int, j int, k int) int {
	eval := cand.evaluation()

	swapOutliers := cand.outliers + cand.swapOutliersChange(row, j, k)
	swapRawEval := cand.rawEvaluation + cand.swapRawEvaluationChange(row, j, k)
	swapEval := cand.computeEvaluation(swapRawEval, swapOutliers)

	return swapEval - eval
}

// Swap element j and k in a certain row.
func (cand *R) swapElems(row int, j int, k int) {
	if cand.Matrix[row][j] == 0 || cand.Matrix[row][k] == 0 {
		panic(fmt.Sprintf("swapping one or more zeros (%d: %d <-> %d)",
			row, j, k))
	}

	cand.rawEvaluation += cand.swapRawEvaluationChange(row, j, k)
	cand.outliers += cand.swapOutliersChange(row, j, k)

	a, b := cand.Matrix[row][j], cand.Matrix[row][k]

	cand.ColSums[j] += b - a
	cand.ColSums[k] += a - b
	cand.Matrix[row][j], cand.Matrix[row][k] = b, a
}

// Make a copy of R.
func (cand R) copy() (result R) {
	result.params = cand.params
	result.expectedColSum = cand.expectedColSum
	result.expectedOutliers = cand.expectedOutliers
	result.outliers = cand.outliers
	result.rawEvaluation = cand.rawEvaluation

	result.Matrix = make([][]int, cand.params.NumNodes)
	for i, row := range cand.Matrix {
		result.Matrix[i] = make([]int, cand.params.NumNodes)
		copy(result.Matrix[i], row)
	}

	result.RowSums = make([]int, cand.params.NumNodes)
	copy(result.RowSums, cand.RowSums)

	result.ColSums = make([]int, cand.params.NumNodes)
	copy(result.ColSums, cand.ColSums)

	return
}

// Build balanced matrix R from RI.
func BuildR(params VbmapParams, ri RI, searchParams SearchParams) (r R, err error) {
	var nonstrictGraph *Graph

	for i := 0; i < searchParams.NumRRetries; i++ {
		activeVbsPerNode := SpreadSum(params.NumVBuckets, params.NumNodes)

		g := buildRFlowGraph(params, ri, activeVbsPerNode)
		feasible, _ := g.FindFeasibleFlow()
		if feasible {
			diag.Printf("Found feasible R after %d attempts", i+1)
			r = graphToR(g, params)
			r.Strict = true
			return
		}

		if searchParams.RelaxMaxVbucketsPerTag && nonstrictGraph == nil {
			relaxMaxVbsPerTag(g)
			feasible, _ := g.FindFeasibleFlow()
			if feasible {
				nonstrictGraph = g
			}
		}
	}

	if nonstrictGraph != nil {
		diag.Printf("Managed to find only non-strictly rack aware R")
		r = graphToR(nonstrictGraph, params)
		r.Strict = false
		return
	}

	err = ErrorNoSolution
	return
}
