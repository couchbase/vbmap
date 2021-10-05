// @author Couchbase <info@couchbase.com>
// @copyright 2015-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.
package main

import (
	"bytes"
	"fmt"
	"math/big"
)

// Matrix R with some meta-information.
type R struct {
	Matrix  [][]int // actual matrix
	RowSums []int   // row sums for the matrix
	ColSums []int   // column sums for the matrix

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
	for range nodes {
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
	for range nodes {
		fmt.Fprintf(buffer, "____")
	}
	fmt.Fprintf(buffer, "|\n")

	fmt.Fprintf(buffer, "    |")
	for i := range nodes {
		fmt.Fprintf(buffer, "%3d ", cand.ColSums[i])
	}
	fmt.Fprintf(buffer, "|\n")
	fmt.Fprintf(buffer, "Evaluation: %d\n", cand.Evaluation())

	return buffer.String()
}

func newRat(num, denom int) *big.Rat {
	return big.NewRat(int64(num), int64(denom))
}

func newRatFromInt(n int) *big.Rat {
	return newRat(n, 1)
}

func ratFloor(r *big.Rat) int {
	num := int(r.Num().Int64())
	denom := int(r.Denom().Int64())

	return num / denom
}

func ratCeil(r *big.Rat) int {
	if r.IsInt() {
		return ratFloor(r)
	}

	return ratFloor(r) + 1
}

func replicasPerSlave(params VbmapParams, strict bool) (*big.Rat, *big.Rat) {
	if params.NumSlaves == 0 {
		return newRatFromInt(0), newRatFromInt(0)
	}

	total := params.NumVBuckets * params.NumReplicas
	perNode := newRat(total, params.NumNodes)

	low := &big.Rat{}
	low.Quo(perNode, newRatFromInt(params.NumSlaves))
	high := low

	if !strict {
		activeVbsPerNode := newRat(params.NumVBuckets, params.NumNodes)
		activeVbsPerNodeLow := ratFloor(activeVbsPerNode)
		activeVbsPerNodeHigh := ratCeil(activeVbsPerNode)

		low = newRatFromInt(activeVbsPerNodeLow)
		low.Mul(low, newRatFromInt(params.NumReplicas))
		low.Quo(low, newRatFromInt(params.NumSlaves))

		high = newRatFromInt(activeVbsPerNodeHigh)
		high.Mul(high, newRatFromInt(params.NumReplicas))
		high.Quo(high, newRatFromInt(params.NumSlaves))
	}

	return low, high
}

func buildRFlowGraph(
	params VbmapParams, ri RI, activeVbs []int, strict bool) (g *Graph) {

	replicasPerSlaveLo, replicasPerSlaveHi :=
		replicasPerSlave(params, strict)

	graphName := fmt.Sprintf("Flow graph for R (%s)", params)
	g = NewGraph(graphName)

	for i, row := range ri.Matrix {
		node := Node(i)
		nodeSrcV := NodeSourceVertex(node)
		nodeSinkV := NodeSinkVertex(node)

		numVbsReplicated := activeVbs[i] * params.NumReplicas
		g.AddEdge(Source, nodeSrcV, numVbsReplicated, numVbsReplicated)

		replications := newRatFromInt(ri.NumInboundReplications(node))

		numReplicas := &big.Rat{}
		numReplicas.Mul(replications, replicasPerSlaveLo)
		numReplicasLo := ratFloor(numReplicas)

		numReplicas.Mul(replications, replicasPerSlaveHi)
		numReplicas.Mul(replications, replicasPerSlaveHi)
		numReplicasHi := ratCeil(numReplicas)

		g.AddEdge(nodeSinkV, Sink, numReplicasHi, numReplicasLo)

		seenTags := make(map[Tag]bool)
		for j, elem := range row {
			if elem == 0 {
				continue
			}

			dstNode := Node(j)
			dstNodeTag := params.Tags[dstNode]

			dstNodeSinkV := NodeSinkVertex(dstNode)
			tagV := TagNodeVertex{dstNodeTag, node}

			if _, seen := seenTags[dstNodeTag]; !seen {
				maxVbsPerTag :=
					numVbsReplicated / params.NumReplicas

				g.AddEdge(nodeSrcV, tagV, maxVbsPerTag, 0)
				seenTags[dstNodeTag] = true
			}

			vbsPerSlave := numVbsReplicated / params.NumSlaves
			g.AddEdge(tagV, dstNodeSinkV,
				vbsPerSlave+1, vbsPerSlave)
		}
	}

	return
}

func graphToR(g *Graph, params VbmapParams) (r *R) {
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

func makeRFromMatrix(params VbmapParams, matrix [][]int) (result *R) {
	result = &R{}
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
			result.outliers++
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
func (cand R) Evaluation() int {
	return cand.computeEvaluation(cand.rawEvaluation, cand.outliers)
}

// Build balanced matrix R from RI.
func BuildR(params VbmapParams, ri RI, searchParams SearchParams) (R, error) {
	for i := 0; i < searchParams.NumRRetries; i++ {
		r := buildR(params, ri, searchParams, true)
		if r != nil {
			diag.Printf("Found feasible R after %d attempts", i+1)
			return *r, nil
		}
	}

	if !searchParams.StrictReplicaBalance {
		r := buildR(params, ri, searchParams, false)
		if r == nil {
			panic("Couldn't generate non-strict R. " +
				"This should not happen")
		}

		return *r, nil
	}

	return R{}, ErrorNoSolution
}

func buildR(
	params VbmapParams, ri RI, searchParams SearchParams, strict bool) *R {

	activeVbsPerNode := SpreadSum(params.NumVBuckets, params.NumNodes)
	Shuffle(activeVbsPerNode)

	g := buildRFlowGraph(params, ri, activeVbsPerNode, strict)
	feasible, _ := g.FindFeasibleFlow()

	dotPath := searchParams.DotPath
	if dotPath != "" {
		err := g.Dot(dotPath, false)
		if err != nil {
			diag.Printf("Couldn't create dot file %s: %s",
				dotPath, err.Error())
		}
	}

	if feasible {
		return graphToR(g, params)
	}

	return nil
}
